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

void RunStockLevel();

void RunDelivery();

void RunOrderStatus();

void RunPayment();

void RunNewOrder();

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

std::vector<double> durations;

void RunBackend(oid_t thread_id) {
  auto txn_count = state.transaction_count;

  UniformGenerator generator;
  Timer<> timer;

  // Start timer
  timer.Reset();
  timer.Start();

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val <= 0.04) {
      RunStockLevel();
    } else if (rng_val <= 0.08) {
      RunDelivery();
    } else if (rng_val <= 0.12) {
      RunOrderStatus();
    } else if (rng_val <= 0.55) {
      RunPayment();
    } else {
      RunNewOrder();
    }

  }

  // Stop timer
  timer.Stop();

  // Set duration
  durations[thread_id] = timer.GetDuration();
}

double RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  double max_duration = std::numeric_limits<double>::min();
  durations.reserve(num_threads);

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(RunBackend, thread_itr));
  }

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Compute max duration
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    max_duration = std::max(max_duration, durations[thread_itr]);
  }

  double throughput = (state.transaction_count * num_threads)/max_duration;

  return throughput;
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

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteTest(executor::AbstractExecutor* executor) {
  time_point_ start, end;
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

void RunNewOrder(){
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

  LOG_INFO("-------------------------------------");

  int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  //int customer_id = GetRandomInteger(0, state.customers_per_district);
  int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);
  //auto o_entry_ts = GetTimeStamp();

  std::vector<int> i_ids, i_w_ids, i_qtys;
  //bool o_all_local = true;

  for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
    i_ids.push_back(GetRandomInteger(0, state.item_count));
    bool remote = GetRandomBoolean(new_order_remote_txns);
    i_w_ids.push_back(warehouse_id);

    if(remote == true) {
      i_w_ids[ol_itr] = GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
      //o_all_local = false;
    }

    i_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
  }

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // getWarehouseTaxRate

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

  auto gwtr_lists_values = ExecuteTest(&warehouse_index_scan_executor);
  if(gwtr_lists_values.empty() == true) {
    LOG_ERROR("getWarehouseTaxRate failed");
    txn_manager.AbortTransaction();
    return;
  }

  auto w_tax = gwtr_lists_values[0][0];
  LOG_TRACE("W_TAX: %d", w_tax);

  // getDistrict

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

  auto gd_lists_values = ExecuteTest(&district_index_scan_executor);
  if(gd_lists_values.empty() == true) {
    LOG_ERROR("getDistrict failed");
    txn_manager.AbortTransaction();
    return;
  }

  auto d_tax = gd_lists_values[0][0];
  LOG_TRACE("D_TAX: %d", d_tax);
  auto d_next_o_id = gd_lists_values[0][1];
  LOG_TRACE("D_NEXT_O_ID: %d", d_next_o_id);

  // incrementNextOrderId

  txn_manager.CommitTransaction();

}

void RunPayment(){
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

}

void RunOrderStatus(){
  /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */

}

void RunDelivery(){
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

}

void RunStockLevel() {
  /*
     "STOCK_LEVEL": {
     "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
     "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
     }
   */

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
