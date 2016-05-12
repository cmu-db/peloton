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

  LOG_INFO("-------------------------------------");

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

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  //std::vector<float> i_prices;
  LOG_INFO("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?");
  for (auto item_id : i_ids) {
    LOG_INFO("item_id: %d", int(item_id));

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

    planner::IndexScanPlan item_index_scan_node(item_table, predicate,
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
  LOG_INFO("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?");
  
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

  LOG_INFO("w_tax: %lf", w_tax.GetDouble());


  // getDistrict
  LOG_INFO("getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?");

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
  LOG_INFO("d_tax: %lf, d_next_o_id: %d", d_tax.GetDouble(), d_next_o_id.GetInteger());
  //LOG_TRACE("D_TAX: %d", d_tax);
  //LOG_TRACE("D_NEXT_O_ID: %d", d_next_o_id);


  // getCustomer
  LOG_INFO("getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");

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
    assert(false);
  }

  // auto c_last = gd_lists_values[0][0];
  // auto c_credit = gd_lists_values[0][1];
  // auto c_discount = gd_lists_values[0][2];

  //LOG_TRACE("D_TAX: %d", d_tax);
  //LOG_TRACE("D_NEXT_O_ID: %d", d_next_o_id);


  // incrementNextOrderId
  LOG_INFO("incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?");

  std::vector<oid_t> district_update_column_ids = {10}; // D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_update_index_scan_node(district_table, predicate,
                                                         district_update_column_ids,
                                                         district_index_scan_desc);
  executor::IndexScanExecutor district_update_index_scan_executor(&district_update_index_scan_node,
                                                           context.get());

  int district_update_value = d_next_o_id.GetInteger() + 1;

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



  LOG_INFO("createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");


  std::unique_ptr<storage::Tuple> orders_tuple(new storage::Tuple(orders_table->GetSchema(), true));

  // O_ID
  orders_tuple->SetValue(0, ValueFactory::GetIntegerValue(d_next_o_id.GetInteger()), nullptr);
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

  
  LOG_INFO("createNewOrder: INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)");
  std::unique_ptr<storage::Tuple> new_order_tuple(new storage::Tuple(new_order_table->GetSchema(), true));

  // NO_O_ID
  new_order_tuple->SetValue(0, ValueFactory::GetIntegerValue(d_next_o_id.GetInteger()), nullptr);
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
    LOG_INFO("getStockInfo: SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?");

    std::vector<oid_t> stock_column_ids = {2, oid_t(3 + district_id), 13, 14, 15, 16}; // S_QUANTITY, S_DIST_%02d, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA

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

    int s_quantity = gsi_lists_values[0][0].GetInteger();

    if (s_quantity >= ol_qty + 10) {
      s_quantity = s_quantity - ol_qty;
    } else {
      s_quantity = s_quantity + 91 - ol_qty;
    }

    Value s_data = gsi_lists_values[0][1];

    int s_ytd = gsi_lists_values[0][2].GetInteger() + ol_qty;

    int s_order_cnt = gsi_lists_values[0][3].GetInteger() + 1;

    int s_remote_cnt = gsi_lists_values[0][4].GetInteger();

    if (ol_w_id != warehouse_id) {
      s_remote_cnt += 1;
    }

    LOG_INFO("updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");

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

    // the original benchmark requires check constraints.
    // however, we ignored here.
    // it does not influence the performance.
    // if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
    // brand_generic = 'B'
    // else:
    // brand_generic = 'G'
        
    LOG_INFO("createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");


    std::unique_ptr<storage::Tuple> order_line_tuple(new storage::Tuple(order_line_table->GetSchema(), true));

    // OL_O_ID
    order_line_tuple->SetValue(0, ValueFactory::GetIntegerValue(d_next_o_id.GetInteger()), nullptr);
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
    // transaction passed commitment.
    // auto d_tax = gd_lists_values[0][0];
    // LOG_TRACE("D_TAX: %d", d_tax);
    // auto d_next_o_id = gd_lists_values[0][1];
    // LOG_TRACE("D_NEXT_O_ID: %d", d_next_o_id);

    return true;
    
  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}

}
}
}