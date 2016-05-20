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
#include "backend/executor/nested_loop_join_executor.h"
#include "backend/executor/aggregate_executor.h"
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
#include "backend/planner/nested_loop_join_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/order_by_plan.h"
#include "backend/planner/limit_plan.h"

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

std::vector<double> durations;

void RunBackend(oid_t thread_id) {
  auto committed_transaction_count = 0;
  UniformGenerator generator;

  auto transaction_count_per_backend = state.transaction_count / state.backend_count;
  bool check_transaction_count = (transaction_count_per_backend != 0);

  Timer<> timer;

  // Start timer
  timer.Reset();
  timer.Start();

  // Run these many transactions
  while (true) {
    // Check run_backends
    if (check_transaction_count == false) {
      if(run_backends == false) {
        break;
      }
    }

    // Check committed_transaction_count
    if (check_transaction_count == true) {
      if(committed_transaction_count == transaction_count_per_backend) {
        break;
      }
    }

    // We only run new order txns
    auto transaction_status = RunNewOrder();

    /*
    auto rng_val = generator.GetSample();
    auto transaction_status = false;

    if (rng_val <= 0.04) {
      transaction_status = RunStockLevel();
    } else if (rng_val <= 0.08) {
      transaction_status = RunDelivery();
    } else if (rng_val <= 0.12) {
      transaction_status = RunOrderStatus();
    } else if (rng_val <= 0.55) {
      transaction_status = RunPayment();
    } else {
      transaction_status = RunNewOrder();
    }
     */

    // Update transaction count if it committed
    if(transaction_status == true){
      committed_transaction_count++;
    }
  }

  // Stop timer
  timer.Stop();

  // Set committed_transaction_count
  transaction_counts[thread_id] = committed_transaction_count;

  // Set duration
  durations[thread_id] = timer.GetDuration();
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  transaction_counts.resize(num_threads);
  durations.resize(num_threads);
  bool check_transaction_count = (state.transaction_count != 0);

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

  // Compute average duration
  double sum_duration = 0;
  double avg_duration = 0;
  for (auto duration : durations) {
    sum_duration += duration;
  }
  avg_duration = sum_duration / num_threads;

  // Compute average throughput and latency
  if(check_transaction_count == false) {
    state.throughput = (sum_transaction_count * 1000)/state.duration;
    state.latency = state.backend_count/state.throughput;
  }
  else {
    state.throughput = state.transaction_count / avg_duration;
  }

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

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////

  int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  // FIXME: minus one here?
  int customer_id = GetRandomInteger(0, state.customers_per_district - 1);
  int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);
  //auto o_entry_ts = GetTimeStamp();

  std::vector<int> i_ids, ol_w_ids, ol_qtys;
  bool o_all_local = true;

  for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
    // in the original TPC-C benchmark, it is possible to read an item that does not exist.
    // for simplicity, we ignore this case.
    // this essentially makes the processing of NewOrder transaction more time-consuming.
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


  //std::vector<float> i_prices;
  LOG_TRACE("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?");
  for (auto item_id : i_ids) {
    LOG_TRACE("item_id: %d", int(item_id));

    //    Planner::IndexScanPlan &item_index_scan_node = BuildItemIndexScanPlan();

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
  warehouse_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

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

  LOG_TRACE("w_tax: %s", w_tax.GetInfo().c_str());

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
  district_key_values.push_back(ValueFactory::GetIntegerValue(district_id));
  district_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

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
    LOG_TRACE("getDistrict failed");
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
  customer_key_values.push_back(ValueFactory::GetIntegerValue(district_id));
  customer_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

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
    LOG_ERROR("getCustomer failed");
    return false;
  }

  if (gc_lists_values.size() != 1) {
    assert(false);
  }

  // incrementNextOrderId
  /*
  LOG_TRACE("incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?");
  // NOTE: why it is different from ycsb update, where all columns ids are in this vector
  std::vector<oid_t> district_update_column_ids = {10}; // D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_update_index_scan_node(district_table, predicate,
                                                         district_update_column_ids,
                                                         district_index_scan_desc);
  executor::IndexScanExecutor district_update_index_scan_executor(&district_update_index_scan_node,
                                                                  context.get());

  int district_update_value = ValuePeeker::PeekAsInteger(d_next_o_id) + 1;

  TargetList district_target_list;
  DirectMapList district_direct_map_list;

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
    LOG_TRACE("incrementNextOrderId failed");
    return false;
  }
  */

  LOG_TRACE("createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");

  std::unique_ptr<storage::Tuple> orders_tuple(new storage::Tuple(orders_table->GetSchema(), true));

  // O_ID
  orders_tuple->SetValue(0, d_next_o_id, nullptr);
  // O_C_ID
  orders_tuple->SetValue(1, ValueFactory::GetIntegerValue(customer_id), nullptr);
  // O_D_ID
  orders_tuple->SetValue(2, ValueFactory::GetIntegerValue(district_id), nullptr);
  // O_W_ID
  orders_tuple->SetValue(3, ValueFactory::GetIntegerValue(warehouse_id), nullptr);
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
  new_order_tuple->SetValue(1, ValueFactory::GetIntegerValue(district_id), nullptr);
  // NO_W_ID
  new_order_tuple->SetValue(2, ValueFactory::GetIntegerValue(warehouse_id), nullptr);

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
    stock_key_values.push_back(ValueFactory::GetIntegerValue(item_id));
    stock_key_values.push_back(ValueFactory::GetIntegerValue(ol_w_id));

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
      LOG_ERROR("getStockInfo failed");
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

    int s_remote_cnt = ValuePeeker::PeekAsInteger(gsi_lists_values[0][4]);

    if (ol_w_id != warehouse_id) {
      s_remote_cnt += 1;
    }

    /*
    LOG_TRACE("updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");

    std::vector<oid_t> stock_update_column_ids = {2, 13, 14, 15}; // S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT

    int s_ytd = ValuePeeker::PeekAsInteger(gsi_lists_values[0][2]) + ol_qty;
    int s_order_cnt = ValuePeeker::PeekAsInteger(gsi_lists_values[0][3]) + 1;

    // Create plan node.
    planner::IndexScanPlan stock_update_index_scan_node(stock_table, predicate,
                                                        stock_update_column_ids,
                                                        stock_index_scan_desc);
    executor::IndexScanExecutor stock_update_index_scan_executor(&stock_update_index_scan_node,
                                                                 context.get());

    TargetList stock_target_list;
    DirectMapList stock_direct_map_list;

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
      LOG_TRACE("updateStock failed");
      return false;
    }
    */

    // the original benchmark requires check constraints.
    // however, we ignored here.
    // it does not influence the performance.
    // if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
    // brand_generic = 'B'
    // else:
    // brand_generic = 'G'

    LOG_TRACE("createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");


    std::unique_ptr<storage::Tuple> order_line_tuple(new storage::Tuple(order_line_table->GetSchema(), true));

    // OL_O_ID
    order_line_tuple->SetValue(0, d_next_o_id, nullptr);
    // OL_D_ID
    order_line_tuple->SetValue(1, ValueFactory::GetIntegerValue(district_id), nullptr);
    // OL_W_ID
    order_line_tuple->SetValue(2, ValueFactory::GetIntegerValue(warehouse_id), nullptr);
    // OL_NUMBER
    order_line_tuple->SetValue(3, ValueFactory::GetIntegerValue(i), nullptr);
    // OL_I_ID
    order_line_tuple->SetValue(4, ValueFactory::GetIntegerValue(item_id), nullptr);
    // OL_SUPPLY_W_ID
    order_line_tuple->SetValue(5, ValueFactory::GetIntegerValue(ol_w_id), nullptr);
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
    LOG_TRACE("D_TAX: %s", gd_lists_values[0][0].GetInfo().c_str());
    LOG_TRACE("D_NEXT_O_ID: %s", gd_lists_values[0][1].GetInfo().c_str());
    LOG_TRACE("transaction committed");
    return true;
  } else {
    PL_ASSERT(result == Result::RESULT_ABORTED ||
              result == Result::RESULT_FAILURE);
    LOG_ERROR("createOrderLine failed");
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

  /*
   * Generate parameter
   */

  int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  int customer_warehouse_id;
  int customer_district_id;
  int customer_id = -1;
  std::string customer_lastname;
  double h_amount = GetRandomFixedPoint(2, payment_min_amount, payment_max_amount);
  // WARN: Hard code the date as 0. may cause problem
  int h_date = 0;

  int x = GetRandomInteger(1, 100);
  int y = GetRandomInteger(1, 100);

  // 85%: paying through own warehouse ( or there is only 1 warehosue)
  if (state.warehouse_count == 1 || x <= 85) {
    customer_warehouse_id = warehouse_id;
    customer_district_id = district_id;
  }
  // 15%: paying through another warehouse
  else {
    customer_warehouse_id = GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
    assert(customer_warehouse_id != warehouse_id);
    customer_district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  }

  // 60%: payment by last name
  if (y <= 60) {
    LOG_INFO("By last name");
    customer_lastname = GetRandomLastName(state.customers_per_district);
  }
  // 40%: payment by id
  else {
    LOG_INFO("By id");
    customer_id = GetRandomInteger(0, state.customers_per_district - 1);
  }

  /*
   * Begin txn and set up the executor context
   */

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Reusable const
  std::vector<expression::AbstractExpression *> runtime_keys;
  auto predicate = nullptr;

  /*
   * Select customer
   */
  std::vector<Value> customer;

  std::vector<oid_t> customer_column_ids =
  {0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20};
  std::vector<oid_t> customer_pkey_column_ids = {0, 1, 2};

  if (customer_id >= 0) {
    // Get customer from ID
    LOG_INFO("getCustomerByCustomerId:  WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ? , # w_id = %d, d_id = %d, c_id = %d",
             warehouse_id, district_id, customer_id);
    std::vector<ExpressionType> customer_pexpr_types;
    std::vector<Value> customer_pkey_values;

    customer_pexpr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_pkey_values.push_back(ValueFactory::GetIntegerValue(customer_id));

    customer_pexpr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_pkey_values.push_back(ValueFactory::GetIntegerValue(district_id));

    customer_pexpr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_pkey_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_pindex_scan_desc(
        customer_pkey_index, customer_pkey_column_ids, customer_pexpr_types,
        customer_pkey_values, runtime_keys
    );

    // Create and set up the index scan executor

    planner::IndexScanPlan customer_pindex_scan_node(
        customer_table, predicate,
        customer_column_ids, customer_pindex_scan_desc
    );

    executor::IndexScanExecutor customer_pindex_scan_executor(&customer_pindex_scan_node, context.get());

    auto customer_list = ExecuteReadTest(&customer_pindex_scan_executor);

    // Check if aborted
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    if (customer_list.size() != 1) {
      assert(false);
      auto result = txn_manager.CommitTransaction();
      if (result == Result::RESULT_SUCCESS) {
        return true;
      } else {
        assert(result == Result::RESULT_ABORTED || result == Result::RESULT_FAILURE);
        return false;
      }
    }

    customer = customer_list[0];
  } else {
    // Get customer by last name
    assert(customer_lastname.empty() == false);
    LOG_INFO("getCustomersByLastName: WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST, # w_id = %d, d_id = %d, c_last = %s",
             warehouse_id, district_id, customer_lastname.c_str());

    // Create and set up the index scan executor
    std::vector<oid_t> customer_key_column_ids = {1, 2, 5};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;

    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(district_id));

    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetStringValue(customer_lastname));

    auto customer_skey_index = customer_table->GetIndexWithOid(customer_table_skey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
        customer_skey_index, customer_key_column_ids, customer_expr_types,
        customer_key_values, runtime_keys
    );

    planner::IndexScanPlan customer_index_scan_node(
        customer_table, predicate,
        customer_column_ids, customer_index_scan_desc
    );

    executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

    // Execute the query
    auto customer_list = ExecuteReadTest(&customer_index_scan_executor);

    // Check if aborted
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    if (customer_list.size() < 1) {
      assert(false);
      auto result = txn_manager.CommitTransaction();
      if (result == Result::RESULT_SUCCESS) {
        return true;
      } else {
        assert(result == Result::RESULT_ABORTED || result == Result::RESULT_FAILURE);
        return false;
      }
    }

    // Get the midpoint customer's id
    auto mid_pos = (customer_list.size() - 1) / 2;
    customer = customer_list[mid_pos];
  }

  /*
   * Select warehouse
   */
  LOG_INFO("getWarehouse:WHERE W_ID = ? # w_id = %d", warehouse_id);

  // Create and set up index scan executor

  // We get the original W_YTD from this query,
  // which is not the TPCC standard
  std::vector<oid_t> warehouse_column_ids =
  {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> warehouse_key_column_ids = {0};
  std::vector<ExpressionType> warehouse_expr_types;
  std::vector<Value> warehouse_key_values;

  warehouse_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  warehouse_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

  auto warehouse_pkey_index = warehouse_table->GetIndexWithOid(warehouse_table_pkey_index_oid);
  planner::IndexScanPlan::IndexScanDesc warehouse_index_scan_desc (
      warehouse_pkey_index, warehouse_key_column_ids, warehouse_expr_types,
      warehouse_key_values, runtime_keys
  );

  planner::IndexScanPlan warehouse_index_scan_node(
      warehouse_table, predicate,
      warehouse_column_ids, warehouse_index_scan_desc
  );
  executor::IndexScanExecutor warehouse_index_scan_executor(&warehouse_index_scan_node, context.get());

  // Execute the query
  auto warehouse_list = ExecuteReadTest(&warehouse_index_scan_executor);

  // Check if aborted
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (warehouse_list.size() != 1) {
    assert(false);
    auto result = txn_manager.CommitTransaction();
    if (result == Result::RESULT_SUCCESS) {
      return true;
    } else {
      assert(result == Result::RESULT_ABORTED || result == Result::RESULT_FAILURE);
      return false;
    }
  }

  /*
   *  Select district
   */

  LOG_INFO("getDistrict: WHERE D_W_ID = ? AND D_ID = ?, # w_id = %d, d_id = %d",
           warehouse_id, district_id);

  // Create and set up index scan executor
  // We also retrieve the original D_YTD from this query,
  // which is not the standard TPCC approach
  std::vector<oid_t> district_column_ids =
  {2, 3, 4, 5, 6, 7, 9};
  std::vector<oid_t> district_key_column_ids = {0, 1};
  std::vector<ExpressionType> district_expr_types;
  std::vector<Value> district_key_values;

  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetIntegerValue(district_id));
  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

  auto district_pkey_index = district_table->GetIndexWithOid(district_table_pkey_index_oid);
  planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
      district_pkey_index, district_key_column_ids, district_expr_types,
      district_key_values, runtime_keys
  );

  planner::IndexScanPlan district_index_scan_node(
      district_table, predicate,
      district_column_ids, district_index_scan_desc
  );
  executor::IndexScanExecutor district_index_scan_executor(&district_index_scan_node, context.get());

  // Execute the query
  auto district_list = ExecuteReadTest(&district_index_scan_executor);

  // Check if aborted
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (district_list.size() != 1) {
    assert(false);
    auto result = txn_manager.CommitTransaction();
    if (result == Result::RESULT_SUCCESS) {
      return true;
    } else {
      assert(result == Result::RESULT_ABORTED || result == Result::RESULT_FAILURE);
      return false;
    }
  }


  /*
   *  Update warehouse balance
   */
  LOG_INFO("updateWarehouseBalance: UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?,# h_amount = %f, w_id = %d",
           h_amount, warehouse_id);
  // Create and setup the update executor
  std::vector<oid_t> warehouse_update_column_ids = {8};
  planner::IndexScanPlan warehouse_update_index_scan_node(
      warehouse_table, predicate, warehouse_update_column_ids,
      warehouse_index_scan_desc
  );
  executor::IndexScanExecutor warehouse_update_index_scan_executor(&warehouse_update_index_scan_node, context.get());

  double warehouse_new_balance = ValuePeeker::PeekDouble(warehouse_list[0][6]) + h_amount;

  TargetList warehouse_target_list;
  DirectMapList warehouse_direct_map_list;

  // Keep the first 8 columns unchanged
  for (oid_t col_itr = 0; col_itr < 8; ++col_itr) {
    warehouse_direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
  }

  // Update the 9th column
  Value warehouse_new_balance_value = ValueFactory::GetDoubleValue(warehouse_new_balance);
  warehouse_target_list.emplace_back(
      8, expression::ExpressionUtil::ConstantValueFactory(warehouse_new_balance_value)
  );

  std::unique_ptr<const planner::ProjectInfo> warehouse_project_info(
      new planner::ProjectInfo(
          std::move(warehouse_target_list),
          std::move(warehouse_direct_map_list)
      )
  );

  planner::UpdatePlan warehouse_update_node(warehouse_table, std::move(warehouse_project_info));

  executor::UpdateExecutor warehouse_update_executor(&warehouse_update_node, context.get());
  warehouse_update_executor.AddChild(&warehouse_update_index_scan_executor);

  // Execute the query
  ExecuteUpdateTest(&warehouse_update_executor);

  // Check if aborted
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  /*
   *  Update district balance
   */
  LOG_INFO("updateDistrictBalance: UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?,# h_amount = %f, d_w_id = %d, d_id = %d",
           h_amount, district_id, warehouse_id);

  // Create and setup the update executor
  std::vector<oid_t> district_update_column_ids = {9};
  planner::IndexScanPlan district_update_index_scan_node(
      district_table, predicate,
      district_update_column_ids, district_index_scan_desc
  );
  executor::IndexScanExecutor district_update_index_scan_executor(&district_update_index_scan_node, context.get());

  double district_new_balance = ValuePeeker::PeekDouble(district_list[0][6]) + h_amount;

  TargetList district_target_list;
  DirectMapList district_direct_map_list;

  // Keep all columns unchanged except for the
  for (oid_t col_itr = 0; col_itr < 11; ++col_itr) {
    if (col_itr != 9) {
      district_direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // Update the 10th column
  Value district_new_balance_value = ValueFactory::GetDoubleValue(district_new_balance);
  district_target_list.emplace_back(
      9, expression::ExpressionUtil::ConstantValueFactory(district_new_balance_value)
  );

  std::unique_ptr<const planner::ProjectInfo> district_project_info(
      new planner::ProjectInfo(
          std::move(district_target_list),
          std::move(district_direct_map_list)
      )
  );

  planner::UpdatePlan district_update_node(district_table, std::move(district_project_info));
  executor::UpdateExecutor district_update_executor(&district_update_node, context.get());
  district_update_executor.AddChild(&district_update_index_scan_executor);

  // Execute the query
  ExecuteUpdateTest(&district_update_executor);

  // Check the result
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  /*
   *  Customer credit information
   */
  std::string customer_credit = ValuePeeker::PeekStringCopyWithoutNull(customer[11]);

  double customer_balance = ValuePeeker::PeekDouble(customer[14]) - h_amount;
  double customer_ytd_payment = ValuePeeker::PeekDouble(customer[15]) + h_amount;
  auto customer_payment_cnt = ValuePeeker::PeekInteger(customer[16]) + 1;
  auto customer_data = GetRandomAlphaNumericString(data_length);

  customer_id = ValuePeeker::PeekInteger(customer[0]);

  // NOTE: Workaround, we assign a constant to the customer's data field
  // auto customer_data = customer_data_constant;

  // Check the credit record of the user
  if (customer_credit == customers_bad_credit) {
    LOG_INFO("updateBCCustomer:# c_balance = %f, c_ytd_payment = %f, c_payment_cnt = %d, c_data = %s, c_w_id = %d, c_d_id = %d, c_id = %d",
             customer_balance, customer_ytd_payment, customer_payment_cnt, customer_data.c_str(),
             customer_warehouse_id, customer_district_id, customer_id);
    std::vector<oid_t> customer_update_column_ids = {16, 17, 18, 20};
    // Construct a new index scan executor
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;

    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_district_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_warehouse_id));

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_update_index_scan_desc(
        customer_pkey_index, customer_pkey_column_ids, customer_expr_types,
        customer_key_values, runtime_keys
    );

    // Create update executor
    planner::IndexScanPlan customer_update_index_scan_node(
        customer_table, predicate, customer_update_column_ids,
        customer_update_index_scan_desc
    );
    executor::IndexScanExecutor customer_update_index_scan_executor(&customer_update_index_scan_node, context.get());

    TargetList customer_target_list;
    DirectMapList customer_direct_map_list;

    // Only update the 17th to 19th and the 21th columns
    for (oid_t col_itr = 0; col_itr < 21; ++col_itr) {
      if ((col_itr >= 16 && col_itr <= 18) || (col_itr == 20)) {
        continue;
      }
      customer_direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
    }
    Value customer_new_balance_value = ValueFactory::GetDoubleValue(customer_balance);
    Value customer_new_ytd_value = ValueFactory::GetDoubleValue(customer_ytd_payment);
    Value customer_new_paycnt_value = ValueFactory::GetIntegerValue(customer_payment_cnt);
    Value customer_new_data_value = ValueFactory::GetStringValue(customer_data);

    customer_target_list.emplace_back(16, expression::ExpressionUtil::ConstantValueFactory(customer_new_balance_value));
    customer_target_list.emplace_back(17, expression::ExpressionUtil::ConstantValueFactory(customer_new_ytd_value));
    customer_target_list.emplace_back(18, expression::ExpressionUtil::ConstantValueFactory(customer_new_paycnt_value));
    customer_target_list.emplace_back(20, expression::ExpressionUtil::ConstantValueFactory(customer_new_data_value));

    std::unique_ptr<const planner::ProjectInfo> customer_project_info(
        new planner::ProjectInfo(
            std::move(customer_target_list),
            std::move(customer_direct_map_list)
        )
    );

    planner::UpdatePlan customer_update_node(customer_table, std::move(customer_project_info));
    executor::UpdateExecutor customer_update_executor(&customer_update_node, context.get());
    customer_update_executor.AddChild(&customer_update_index_scan_executor);

    // Execute the query
    ExecuteUpdateTest(&customer_update_executor);
  }
  else {
    LOG_INFO("updateGCCustomer: # c_balance = %f, c_ytd_payment = %f, c_payment_cnt = %d, c_w_id = %d, c_d_id = %d, c_id = %d",
             customer_balance, customer_ytd_payment, customer_payment_cnt,
             customer_warehouse_id, customer_district_id, customer_id);
    // Create update executor
    std::vector<oid_t> customer_update_column_ids = {16, 17, 18};

    // Construct a new index scan executor
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;

    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_district_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(customer_warehouse_id));

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_update_index_scan_desc(
        customer_pkey_index, customer_pkey_column_ids, customer_expr_types,
        customer_key_values, runtime_keys
    );

    // Create update executor
    planner::IndexScanPlan customer_update_index_scan_node(
        customer_table, predicate, customer_update_column_ids,
        customer_update_index_scan_desc
    );

    executor::IndexScanExecutor customer_update_index_scan_executor(&customer_update_index_scan_node, context.get());

    TargetList customer_target_list;
    DirectMapList customer_direct_map_list;

    // Only update the 17th to 19th columns
    for (oid_t col_itr = 0; col_itr < 21; ++col_itr) {
      if (col_itr >= 16 && col_itr <= 18) {
        continue;
      }
      customer_direct_map_list.emplace_back(col_itr, std::pair<oid_t, oid_t>(0, col_itr));
    }
    Value customer_new_balance_value = ValueFactory::GetDoubleValue(customer_balance);
    Value customer_new_ytd_value = ValueFactory::GetDoubleValue(customer_ytd_payment);
    Value customer_new_paycnt_value = ValueFactory::GetIntegerValue(customer_payment_cnt);

    customer_target_list.emplace_back(16, expression::ExpressionUtil::ConstantValueFactory(customer_new_balance_value));
    customer_target_list.emplace_back(17, expression::ExpressionUtil::ConstantValueFactory(customer_new_ytd_value));
    customer_target_list.emplace_back(18, expression::ExpressionUtil::ConstantValueFactory(customer_new_paycnt_value));

    std::unique_ptr<const planner::ProjectInfo> customer_project_info(
        new planner::ProjectInfo(
            std::move(customer_target_list),
            std::move(customer_direct_map_list)
        )
    );

    planner::UpdatePlan customer_update_node(customer_table, std::move(customer_project_info));
    executor::UpdateExecutor customer_update_executor(&customer_update_node, context.get());
    customer_update_executor.AddChild(&customer_update_index_scan_executor);

    // Execute the query
    ExecuteUpdateTest(&customer_update_executor);
  }

  // Check the result
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  /*
   *  Insert History
   */
  LOG_INFO("insertHistory: INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
  std::unique_ptr<storage::Tuple> history_tuple(new storage::Tuple(history_table->GetSchema(), true));
  auto h_data = GetRandomAlphaNumericString(history_data_length);

  // H_C_ID
  history_tuple->SetValue(0, ValueFactory::GetIntegerValue(customer_id), nullptr);
  // H_C_D_ID
  history_tuple->SetValue(1, ValueFactory::GetIntegerValue(customer_district_id), nullptr);
  // H_C_W_ID
  history_tuple->SetValue(2, ValueFactory::GetIntegerValue(customer_warehouse_id), nullptr);
  // H_D_ID
  history_tuple->SetValue(3, ValueFactory::GetIntegerValue(district_id), nullptr);
  // H_W_ID
  history_tuple->SetValue(4, ValueFactory::GetIntegerValue(warehouse_id), nullptr);
  // H_DATE
  history_tuple->SetValue(5, ValueFactory::GetTimestampValue(h_date), nullptr);
  // H_AMOUNT
  history_tuple->SetValue(6, ValueFactory::GetDoubleValue(h_amount), nullptr);
  // H_DATA
  history_tuple->SetValue(7, ValueFactory::GetStringValue(h_data), context.get()->GetExecutorContextPool());

  planner::InsertPlan history_insert_node(history_table, std::move(history_tuple));
  executor::InsertExecutor history_insert_executor(&history_insert_node, context.get());

  // Execute
  history_insert_executor.Execute();

  // Check result
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    assert(false);
    txn_manager.AbortTransaction();
    return false;
  }

  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.CommitTransaction();
  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    assert(result == Result::RESULT_ABORTED || result == Result::RESULT_FAILURE);
    return false;
  }

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
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
  std::vector<oid_t> orders_column_ids = {COL_IDX_O_ID, COL_IDX_O_CARRIER_ID, COL_IDX_O_ENTRY_D};
  std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_W_ID, COL_IDX_O_D_ID, COL_IDX_O_C_ID};
  std::vector<ExpressionType> orders_expr_types;
  std::vector<Value> orders_key_values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
    order_line_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    customer_expr_types.push_back(
        ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
  orders_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
    order_line_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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

  return true;
}

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
  district_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetIntegerValue(d_id));

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
  order_line_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  order_line_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
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
  stock_key_values.push_back(ValueFactory::GetIntegerValue(w_id));

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

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
