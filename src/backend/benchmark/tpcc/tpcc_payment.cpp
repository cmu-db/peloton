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

  LOG_INFO("-------------------------------------");

  /*
   * Generate parameter
   */

  int warehouse_id = GetRandomInteger(0, state.warehouse_count - 1);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  int customer_warehouse_id;
  int customer_district_id;
  int customer_id = -1;
  std::string customer_lastname;
  auto h_amount = GetRandomFixedPoint(2, payment_min_amount, payment_max_amount);
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

  double warehouse_new_balance = warehouse_list[0][6].GetDouble() + h_amount;

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

  double district_new_balance = district_list[0][6].GetDouble() + h_amount;

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
  std::string customer_credit;
  GetStringFromValue(customer[11], customer_credit);

  auto customer_balance = customer[14].GetDouble() - h_amount;
  auto customer_ytd_payment = customer[15].GetDouble() + h_amount;
  auto customer_payment_cnt = customer[16].GetInteger() + 1;
  customer_id = customer[0].GetInteger();
  // NOTE: Workaround, we assign a constant to the customer's data field
  // auto customer_data = customer_data_constant;

  // Check the credit record of the user
  if (customer_credit == customers_bad_credit) {
    LOG_INFO("updateBCCustomer:# c_balance = %f, c_ytd_payment = %f, c_payment_cnt = %d, c_data = %s, c_w_id = %d, c_d_id = %d, c_id = %d",
               customer_balance, customer_ytd_payment, customer_payment_cnt, data_constant.c_str(),
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
    Value customer_new_data_value = ValueFactory::GetStringValue(data_constant.c_str());

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

  // Note: workaround
  // auto h_data = data_constant;

  // H_C_ID
  history_tuple->SetValue(0, ValueFactory::GetIntegerValue(customer_id), nullptr);
  // H_C_D_ID
  history_tuple->SetValue(1, ValueFactory::GetTinyIntValue(customer_district_id), nullptr);
  // H_C_W_ID
  history_tuple->SetValue(2, ValueFactory::GetSmallIntValue(customer_warehouse_id), nullptr);
  // H_D_ID
  history_tuple->SetValue(3, ValueFactory::GetTinyIntValue(district_id), nullptr);
  // H_W_ID
  history_tuple->SetValue(4, ValueFactory::GetSmallIntValue(warehouse_id), nullptr);
  // H_DATE
  history_tuple->SetValue(5, ValueFactory::GetTimestampValue(h_date), nullptr);
  // H_AMOUNT
  history_tuple->SetValue(6, ValueFactory::GetDoubleValue(h_amount), nullptr);
  // H_DATA
  history_tuple->SetValue(7, ValueFactory::GetStringValue(data_constant), context.get()->GetExecutorContextPool());

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
}

}
}
}