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
namespace tpcc {

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

bool RunStockLevel();

bool RunDelivery();

bool RunOrderStatus();

bool RunPayment();

bool RunNewOrder();

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

// Used to control backend execution
volatile bool run_backends = true;

// Committed transaction counts
std::vector<double> transaction_counts;

void RunBackend(oid_t thread_id) {
  auto committed_transaction_count = 0;

  // Run these many transactions
  while (true) {
    // Check if the backend should stop
    if (run_backends == false) {
      break;
    }

    // We only run new order txns
    auto transaction_status = RunNewOrder();

    // Update transaction count if it committed
    if(transaction_status == true){
      committed_transaction_count++;
    }
  }

  // Set committed_transaction_count
  transaction_counts[thread_id] = committed_transaction_count;
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  transaction_counts.resize(num_threads);

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  // Sleep for duration specified by user and then stop the backends
  auto sleep_period = std::chrono::milliseconds(state.duration);
  std::this_thread::sleep_for(sleep_period);
  run_backends = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Compute total committed transactions
  auto sum_transaction_count = 0;
  for(auto transaction_count : transaction_counts){
    sum_transaction_count += transaction_count;
  }

  // Compute average throughput and latency
  state.throughput = (sum_transaction_count * 1000)/state.duration;
  state.latency = state.backend_count/state.throughput;
}

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteTest(executor::AbstractExecutor* executor) {
  bool status = false;

  // Run all the executors
  status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(
        executor->GetOutput());

    if(result_tile == nullptr)
      break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}


std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor) {
  // Run all the executors
  bool status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if(result_tile == nullptr)
      break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor* executor) {
  // Run all the executors
  bool status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  // Execute stuff
  while (executor->Execute() == true);
}

bool RunNewOrder(){
   /*
     "NEW_ORDER": {
     "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
     "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
     "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
     "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
     "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
     "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
     "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
     "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
     "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
     "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
     }
   */

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////

  int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  int customer_id = GetRandomInteger(0, state.customers_per_district - 1);
  int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

  std::vector<int> i_ids, ol_w_ids, ol_qtys;
  bool o_all_local = true;

  for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
    i_ids.push_back(GetRandomInteger(0, state.item_count - 1));
    bool remote = GetRandomBoolean(new_order_remote_txns);
    ol_w_ids.push_back(warehouse_id);

    if(remote == true) {
      ol_w_ids[ol_itr] = GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
      o_all_local = false;
    }

    ol_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
  }

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  LOG_TRACE("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?");
  for (auto item_id : i_ids) {
    LOG_TRACE("item_id: %d", int(item_id));

    std::vector<oid_t> item_column_ids = {2, 3, 4}; // I_NAME, I_PRICE, I_DATA

    // Create and set up index scan executor
    std::vector<oid_t> item_key_column_ids = {0}; // I_ID
    std::vector<ExpressionType> item_expr_types;
    std::vector<Value> item_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    item_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    item_key_values.push_back(ValueFactory::GetIntegerValue(item_id));

    auto item_pkey_index = item_table->GetIndexWithOid(
        item_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc item_index_scan_desc(
        item_pkey_index, item_key_column_ids, item_expr_types,
        item_key_values, runtime_keys);

    // Create plan node.
    auto predicate = nullptr;

    static planner::IndexScanPlan item_index_scan_node(item_table, predicate,
                                                       item_column_ids,
                                                       item_index_scan_desc);

    executor::IndexScanExecutor item_index_scan_executor(&item_index_scan_node,
                                                         context.get());

    auto gii_lists_values = ExecuteReadTest(&item_index_scan_executor);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    if (gii_lists_values.size() != 1) {
      assert(false);
    }

  }

  // getWarehouseTaxRate
  LOG_TRACE("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?");

  std::vector<oid_t> warehouse_column_ids = {7}; // W_TAX

  // Create and set up index scan executor
  std::vector<oid_t> warehouse_key_column_ids = {0}; // W_ID
  std::vector<ExpressionType> warehouse_expr_types;
  std::vector<Value> warehouse_key_values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  warehouse_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  warehouse_key_values.push_back(ValueFactory::GetSmallIntValue(warehouse_id));

  auto warehouse_pkey_index = warehouse_table->GetIndexWithOid(
      warehouse_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc warehouse_index_scan_desc(
      warehouse_pkey_index, warehouse_key_column_ids, warehouse_expr_types,
      warehouse_key_values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan warehouse_index_scan_node(warehouse_table, predicate,
                                                   warehouse_column_ids,
                                                   warehouse_index_scan_desc);

  executor::IndexScanExecutor warehouse_index_scan_executor(&warehouse_index_scan_node,
                                                            context.get());

  auto gwtr_lists_values = ExecuteReadTest(&warehouse_index_scan_executor);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (gwtr_lists_values.size() != 1) {
    assert(false);
  }

  auto w_tax = gwtr_lists_values[0][0];

  LOG_TRACE("W_TAX: %s", w_tax.GetInfo().c_str());


  // getDistrict
  LOG_TRACE("getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?");

  std::vector<oid_t> district_column_ids = {8, 10}; // D_TAX, D_NEXT_O_ID

  // Create and set up index scan executor
  std::vector<oid_t> district_key_column_ids = {0, 1}; // D_ID, D_W_ID
  std::vector<ExpressionType> district_expr_types;
  std::vector<Value> district_key_values;

  district_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetTinyIntValue(district_id));
  district_key_values.push_back(ValueFactory::GetSmallIntValue(warehouse_id));

  auto district_pkey_index = district_table->GetIndexWithOid(
      district_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
      district_pkey_index, district_key_column_ids, district_expr_types,
      district_key_values, runtime_keys);

  // Create plan node.
  planner::IndexScanPlan district_index_scan_node(district_table, predicate,
                                                  district_column_ids,
                                                  district_index_scan_desc);
  executor::IndexScanExecutor district_index_scan_executor(&district_index_scan_node,
                                                           context.get());

  auto gd_lists_values = ExecuteReadTest(&district_index_scan_executor);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (gd_lists_values.size() != 1) {
    assert(false);
  }

  auto d_tax = gd_lists_values[0][0];
  auto d_next_o_id = gd_lists_values[0][1];
  LOG_TRACE("D_TAX: %s, D_NEXT_O_ID: %s", d_tax.GetInfo().c_str(), d_next_o_id.GetInfo().c_str());


  // getCustomer
  LOG_TRACE("getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");

  std::vector<oid_t> customer_column_ids = {5, 13, 15}; // C_LAST, C_CREDIT, C_DISCOUNT

  // Create and set up index scan executor
  std::vector<oid_t> customer_key_column_ids = {0, 1, 2}; // C_ID, C_D_ID, C_W_ID
  std::vector<ExpressionType> customer_expr_types;
  std::vector<Value> customer_key_values;

  customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_id));
  customer_key_values.push_back(ValueFactory::GetTinyIntValue(district_id));
  customer_key_values.push_back(ValueFactory::GetSmallIntValue(warehouse_id));

  auto customer_pkey_index = customer_table->GetIndexWithOid(
      customer_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
      customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

  // Create plan node.
  planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
                                                  customer_column_ids,
                                                  customer_index_scan_desc);
  executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node,
                                                           context.get());

  auto gc_lists_values = ExecuteReadTest(&customer_index_scan_executor);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (gc_lists_values.size() != 1) {
    LOG_ERROR("Could not find customer : %d", customer_id);
    return false;
  }

  // auto c_last = gd_lists_values[0][0];
  // auto c_credit = gd_lists_values[0][1];
  // auto c_discount = gd_lists_values[0][2];

  // incrementNextOrderId
  LOG_TRACE("incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?");

  std::vector<oid_t> district_update_column_ids = {10}; // D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_update_index_scan_node(district_table, predicate,
                                                         district_update_column_ids,
                                                         district_index_scan_desc);
  executor::IndexScanExecutor district_update_index_scan_executor(&district_update_index_scan_node,
                                                                  context.get());

  int district_update_value = ValuePeeker::PeekAsInteger(d_next_o_id) + 1;

  planner::ProjectInfo::TargetList district_target_list;
  planner::ProjectInfo::DirectMapList district_direct_map_list;

  // Update the last attribute
  for (oid_t col_itr = 0; col_itr < 10; col_itr++) {
    district_direct_map_list.emplace_back(col_itr,
                                          std::pair<oid_t, oid_t>(0, col_itr));
  }

  Value district_update_val = ValueFactory::GetIntegerValue(district_update_value);
  district_target_list.emplace_back(
      10, expression::ExpressionUtil::ConstantValueFactory(district_update_val));

  std::unique_ptr<const planner::ProjectInfo> district_project_info(
      new planner::ProjectInfo(std::move(district_target_list),
                               std::move(district_direct_map_list)));
  planner::UpdatePlan district_update_node(district_table, std::move(district_project_info));

  executor::UpdateExecutor district_update_executor(&district_update_node, context.get());
  district_update_executor.AddChild(&district_update_index_scan_executor);

  ExecuteUpdateTest(&district_update_executor);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  LOG_TRACE("createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");

  std::unique_ptr<storage::Tuple> orders_tuple(new storage::Tuple(orders_table->GetSchema(), true));

  // O_ID
  orders_tuple->SetValue(0, d_next_o_id, nullptr);
  // O_C_ID
  orders_tuple->SetValue(1, ValueFactory::GetIntegerValue(customer_id), nullptr);
  // O_D_ID
  orders_tuple->SetValue(2, ValueFactory::GetTinyIntValue(district_id), nullptr);
  // O_W_ID
  orders_tuple->SetValue(3, ValueFactory::GetSmallIntValue(warehouse_id), nullptr);
  // O_ENTRY_D
  //auto o_entry_d = GetTimeStamp();
  orders_tuple->SetValue(4, ValueFactory::GetTimestampValue(1) , nullptr);
  // O_CARRIER_ID
  orders_tuple->SetValue(5, ValueFactory::GetIntegerValue(0), nullptr);
  // O_OL_CNT
  orders_tuple->SetValue(6, ValueFactory::GetIntegerValue(o_ol_cnt), nullptr);
  // O_ALL_LOCAL
  orders_tuple->SetValue(7, ValueFactory::GetIntegerValue(o_all_local), nullptr);

  planner::InsertPlan orders_node(orders_table, std::move(orders_tuple));
  executor::InsertExecutor orders_executor(&orders_node, context.get());
  orders_executor.Execute();


  LOG_TRACE("createNewOrder: INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)");
  std::unique_ptr<storage::Tuple> new_order_tuple(new storage::Tuple(new_order_table->GetSchema(), true));

  // NO_O_ID
  new_order_tuple->SetValue(0, d_next_o_id, nullptr);
  // NO_D_ID
  new_order_tuple->SetValue(1, ValueFactory::GetTinyIntValue(district_id), nullptr);
  // NO_W_ID
  new_order_tuple->SetValue(2, ValueFactory::GetSmallIntValue(warehouse_id), nullptr);

  planner::InsertPlan new_order_node(new_order_table, std::move(new_order_tuple));
  executor::InsertExecutor new_order_executor(&new_order_node, context.get());
  new_order_executor.Execute();

  for (size_t i = 0; i < i_ids.size(); ++i) {
    int item_id = i_ids.at(i);
    int ol_w_id = ol_w_ids.at(i);
    int ol_qty = ol_qtys.at(i);

    LOG_TRACE("getStockInfo: SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_? FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?");

    // S_QUANTITY, S_DIST_%02d, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA
    std::vector<oid_t> stock_column_ids = {2, oid_t(3 + district_id), 13, 14, 15, 16};

    // Create and set up index scan executor
    std::vector<oid_t> stock_key_column_ids = {0, 1}; // S_I_ID, S_W_ID
    std::vector<ExpressionType> stock_expr_types;
    std::vector<Value> stock_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    stock_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    stock_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    stock_key_values.push_back(ValueFactory::GetTinyIntValue(item_id));
    stock_key_values.push_back(ValueFactory::GetSmallIntValue(ol_w_id));

    auto stock_pkey_index = stock_table->GetIndexWithOid(
        stock_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
        stock_pkey_index, stock_key_column_ids, stock_expr_types,
        stock_key_values, runtime_keys);

    // Create plan node.
    auto predicate = nullptr;

    planner::IndexScanPlan stock_index_scan_node(stock_table, predicate,
                                                 stock_column_ids,
                                                 stock_index_scan_desc);

    executor::IndexScanExecutor stock_index_scan_executor(&stock_index_scan_node,
                                                          context.get());

    auto gsi_lists_values = ExecuteReadTest(&stock_index_scan_executor);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    if (gsi_lists_values.size() != 1) {
      assert(false);
    }

    int s_quantity = ValuePeeker::PeekAsInteger(gsi_lists_values[0][0]);

    if (s_quantity >= ol_qty + 10) {
      s_quantity = s_quantity - ol_qty;
    } else {
      s_quantity = s_quantity + 91 - ol_qty;
    }

    Value s_data = gsi_lists_values[0][1];

    int s_ytd = ValuePeeker::PeekAsInteger(gsi_lists_values[0][2]) + ol_qty;

    int s_order_cnt = ValuePeeker::PeekAsInteger(gsi_lists_values[0][3]) + 1;

    int s_remote_cnt = ValuePeeker::PeekAsInteger(gsi_lists_values[0][4]);

    if (ol_w_id != warehouse_id) {
      s_remote_cnt += 1;
    }

    LOG_TRACE("updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");

    std::vector<oid_t> stock_update_column_ids = {2, 13, 14, 15}; // S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT

    // Create plan node.
    planner::IndexScanPlan stock_update_index_scan_node(stock_table, predicate,
                                                        stock_update_column_ids,
                                                        stock_index_scan_desc);
    executor::IndexScanExecutor stock_update_index_scan_executor(&stock_update_index_scan_node,
                                                                 context.get());

    planner::ProjectInfo::TargetList stock_target_list;
    planner::ProjectInfo::DirectMapList stock_direct_map_list;

    // Update the last attribute
    for (oid_t col_itr = 0; col_itr < 17; col_itr++) {
      if (col_itr != 2 && col_itr != 13 && col_itr != 14 && col_itr != 15) {
        stock_direct_map_list.emplace_back(col_itr,
                                           std::pair<oid_t, oid_t>(0, col_itr));
      }
    }

    stock_target_list.emplace_back(
        2, expression::ExpressionUtil::ConstantValueFactory(ValueFactory::GetIntegerValue(s_quantity)));
    stock_target_list.emplace_back(
        13, expression::ExpressionUtil::ConstantValueFactory(ValueFactory::GetIntegerValue(s_ytd)));
    stock_target_list.emplace_back(
        14, expression::ExpressionUtil::ConstantValueFactory(ValueFactory::GetIntegerValue(s_order_cnt)));
    stock_target_list.emplace_back(
        15, expression::ExpressionUtil::ConstantValueFactory(ValueFactory::GetIntegerValue(s_remote_cnt)));

    std::unique_ptr<const planner::ProjectInfo> stock_project_info(
        new planner::ProjectInfo(std::move(stock_target_list),
                                 std::move(stock_direct_map_list)));
    planner::UpdatePlan stock_update_node(stock_table, std::move(stock_project_info));

    executor::UpdateExecutor stock_update_executor(&stock_update_node, context.get());
    stock_update_executor.AddChild(&stock_update_index_scan_executor);

    ExecuteUpdateTest(&stock_update_executor);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    LOG_TRACE("createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    std::unique_ptr<storage::Tuple> order_line_tuple(new storage::Tuple(order_line_table->GetSchema(), true));

    // OL_O_ID
    order_line_tuple->SetValue(0, d_next_o_id, nullptr);
    // OL_D_ID
    order_line_tuple->SetValue(1, ValueFactory::GetTinyIntValue(district_id), nullptr);
    // OL_W_ID
    order_line_tuple->SetValue(2, ValueFactory::GetSmallIntValue(warehouse_id), nullptr);
    // OL_NUMBER
    order_line_tuple->SetValue(3, ValueFactory::GetIntegerValue(i), nullptr);
    // OL_I_ID
    order_line_tuple->SetValue(4, ValueFactory::GetIntegerValue(item_id), nullptr);
    // OL_SUPPLY_W_ID
    order_line_tuple->SetValue(5, ValueFactory::GetSmallIntValue(ol_w_id), nullptr);
    // OL_DELIVERY_D
    order_line_tuple->SetValue(6, ValueFactory::GetTimestampValue(1) , nullptr);
    // OL_QUANTITY
    order_line_tuple->SetValue(7, ValueFactory::GetIntegerValue(ol_qty), nullptr);
    // OL_AMOUNT
    // TODO: workaround!!! I don't know how to get float from Value.
    order_line_tuple->SetValue(8, ValueFactory::GetDoubleValue(0), nullptr);
    // OL_DIST_INFO
    order_line_tuple->SetValue(9, s_data, nullptr);

    planner::InsertPlan order_line_node(order_line_table, std::move(order_line_tuple));
    executor::InsertExecutor order_line_executor(&order_line_node, context.get());
    order_line_executor.Execute();

  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    LOG_TRACE("D_TAX: %d", gd_lists_values[0][0]);
    LOG_TRACE("D_NEXT_O_ID: %d", gd_lists_values[0][1]);

    return true;
  } else {
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}

bool RunPayment(){
  /*
     "PAYMENT": {
     "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
     "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
     "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
     "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?", # h_amount, d_w_id, d_id
     "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
     "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
     "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
     "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
     "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
     }
   */

  LOG_TRACE("-------------------------------------");

  //int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  //int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  return true;
}

bool RunOrderStatus(){
  /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */
  return true;
}

bool RunDelivery(){
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
  return true;
}

bool RunStockLevel() {
  /*
     "STOCK_LEVEL": {
     "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
     "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
     }
   */
  return true;
}


/////////////////////////////////////////////////////////
// TABLES
/////////////////////////////////////////////////////////

/*
   CREATE TABLE WAREHOUSE (
   0 W_ID SMALLINT DEFAULT '0' NOT NULL,
   1 W_NAME VARCHAR(16) DEFAULT NULL,
   2 W_STREET_1 VARCHAR(32) DEFAULT NULL,
   3 W_STREET_2 VARCHAR(32) DEFAULT NULL,
   4 W_CITY VARCHAR(32) DEFAULT NULL,
   5 W_STATE VARCHAR(2) DEFAULT NULL,
   6 W_ZIP VARCHAR(9) DEFAULT NULL,
   7 W_TAX FLOAT DEFAULT NULL,
   8 W_YTD FLOAT DEFAULT NULL,
   CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
   );

   INDEXES:
   0 W_ID
 */

/*
   CREATE TABLE DISTRICT (
   0 D_ID TINYINT DEFAULT '0' NOT NULL,
   1 D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
   2 D_NAME VARCHAR(16) DEFAULT NULL,
   3 D_STREET_1 VARCHAR(32) DEFAULT NULL,
   4 D_STREET_2 VARCHAR(32) DEFAULT NULL,
   5 D_CITY VARCHAR(32) DEFAULT NULL,
   6 D_STATE VARCHAR(2) DEFAULT NULL,
   7 D_ZIP VARCHAR(9) DEFAULT NULL,
   8 D_TAX FLOAT DEFAULT NULL,
   9 D_YTD FLOAT DEFAULT NULL,
   10 D_NEXT_O_ID INT DEFAULT NULL,
   PRIMARY KEY (D_W_ID,D_ID)
   );

   INDEXES:
   0, 1 D_ID, D_W_ID
 */

/*
   CREATE TABLE ITEM (
   0 I_ID INTEGER DEFAULT '0' NOT NULL,
   1 I_IM_ID INTEGER DEFAULT NULL,
   2 I_NAME VARCHAR(32) DEFAULT NULL,
   3 I_PRICE FLOAT DEFAULT NULL,
   4 I_DATA VARCHAR(64) DEFAULT NULL,
   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
   );

   INDEXES:
   0 I_ID
 */
/*
     CREATE TABLE CUSTOMER (
     0  C_ID INTEGER DEFAULT '0' NOT NULL,
     1  C_D_ID TINYINT DEFAULT '0' NOT NULL,
     2  C_W_ID SMALLINT DEFAULT '0' NOT NULL,
     3  C_FIRST VARCHAR(32) DEFAULT NULL,
     4  C_MIDDLE VARCHAR(2) DEFAULT NULL,
     5  C_LAST VARCHAR(32) DEFAULT NULL,
     6  C_STREET_1 VARCHAR(32) DEFAULT NULL,
     7  C_STREET_2 VARCHAR(32) DEFAULT NULL,
     8  C_CITY VARCHAR(32) DEFAULT NULL,
     9  C_STATE VARCHAR(2) DEFAULT NULL,
     10 C_ZIP VARCHAR(9) DEFAULT NULL,
     11 C_PHONE VARCHAR(32) DEFAULT NULL,
     12 C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
     13 C_CREDIT VARCHAR(2) DEFAULT NULL,
     14 C_CREDIT_LIM FLOAT DEFAULT NULL,
     15 C_DISCOUNT FLOAT DEFAULT NULL,
     16 C_BALANCE FLOAT DEFAULT NULL,
     17 C_YTD_PAYMENT FLOAT DEFAULT NULL,
     18 C_PAYMENT_CNT INTEGER DEFAULT NULL,
     19 C_DELIVERY_CNT INTEGER DEFAULT NULL,
     20 C_DATA VARCHAR(500),
     PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
     UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
     CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
     );
     CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);

     INDEXES:
     0, 1, 2 C_ID, C_W_ID, C_D_ID
     1, 2, 5 C_W_ID, C_D_ID, C_LAST
 */
/*
    CREATE TABLE HISTORY (
    0 H_C_ID INTEGER DEFAULT NULL,
    1 H_C_D_ID TINYINT DEFAULT NULL,
    2 H_C_W_ID SMALLINT DEFAULT NULL,
    3 H_D_ID TINYINT DEFAULT NULL,
    4 H_W_ID SMALLINT DEFAULT '0' NOT NULL,
    5 H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    6 H_AMOUNT FLOAT DEFAULT NULL,
    7 H_DATA VARCHAR(32) DEFAULT NULL,
    CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
    CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
    );
 */
/*
   CREATE TABLE STOCK (
   0  S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
   1  S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
   2  S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
   3  S_DIST_01 VARCHAR(32) DEFAULT NULL,
   4  S_DIST_02 VARCHAR(32) DEFAULT NULL,
   5  S_DIST_03 VARCHAR(32) DEFAULT NULL,
   6  S_DIST_04 VARCHAR(32) DEFAULT NULL,
   7  S_DIST_05 VARCHAR(32) DEFAULT NULL,
   8  S_DIST_06 VARCHAR(32) DEFAULT NULL,
   9  S_DIST_07 VARCHAR(32) DEFAULT NULL,
   10 S_DIST_08 VARCHAR(32) DEFAULT NULL,
   11 S_DIST_09 VARCHAR(32) DEFAULT NULL,
   12 S_DIST_10 VARCHAR(32) DEFAULT NULL,
   13 S_YTD INTEGER DEFAULT NULL,
   14 S_ORDER_CNT INTEGER DEFAULT NULL,
   15 S_REMOTE_CNT INTEGER DEFAULT NULL,
   16 S_DATA VARCHAR(64) DEFAULT NULL,
   PRIMARY KEY (S_W_ID,S_I_ID)
   );

   INDEXES:
   0, 1 S_I_ID, S_W_ID
 */
/*
   CREATE TABLE ORDERS (
   0 O_ID INTEGER DEFAULT '0' NOT NULL,
   1 O_C_ID INTEGER DEFAULT NULL,
   2 O_D_ID TINYINT DEFAULT '0' NOT NULL,
   3 O_W_ID SMALLINT DEFAULT '0' NOT NULL,
   4 O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   5 O_CARRIER_ID INTEGER DEFAULT NULL,
   6 O_OL_CNT INTEGER DEFAULT NULL,
   7 O_ALL_LOCAL INTEGER DEFAULT NULL,
   PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
   UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
   );
   CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);

   INDEXES:
   0, 2, 3 O_ID, O_D_ID, O_W_ID
   1, 2, 3 O_C_ID, O_D_ID, O_W_ID
 */
/*
   CREATE TABLE NEW_ORDER (
   0 NO_O_ID INTEGER DEFAULT '0' NOT NULL,
   1 NO_D_ID TINYINT DEFAULT '0' NOT NULL,
   2 NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
   );

   INDEXES:
   0, 1, 2 NO_O_ID, NO_D_ID, NO_W_ID
 */
/*
   CREATE TABLE ORDER_LINE (
   0 OL_O_ID INTEGER DEFAULT '0' NOT NULL,
   1 OL_D_ID TINYINT DEFAULT '0' NOT NULL,
   2 OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
   3 OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
   4 OL_I_ID INTEGER DEFAULT NULL,
   5 OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
   6 OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
   7 OL_QUANTITY INTEGER DEFAULT NULL,
   8 OL_AMOUNT FLOAT DEFAULT NULL,
   9 OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
   PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
   );
   CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);

   INDEXES:
   0, 1, 2, 3 OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER
   0, 1, 2 OL_O_ID, OL_D_ID, OL_W_ID
 */

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
