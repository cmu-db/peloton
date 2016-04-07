//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// loader.cpp
//
// Identification: benchmark/tpcc/loader.cpp
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

#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/index/index_factory.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

storage::Database* tpcc_database;
storage::DataTable* warehouse_table;
storage::DataTable* district_table;
storage::DataTable* item_table;
storage::DataTable* customer_table;
storage::DataTable* history_table;
storage::DataTable* stock_table;
storage::DataTable* orders_table;
storage::DataTable* new_order_table;
storage::DataTable* order_line_table;

void CreateWarehouseTable() {
}

void CreateDistrictTable() {
}

void CreateItemTable() {
}

void CreateCustomerTable() {
}

void CreateHistoryTable() {
}

void CreateStockTable() {
}

void CreateNewOrderTable() {
}

void CreateOrderLineTable() {
}

void CreateTPCCDatabase() {
  /////////////////////////////////////////////////////////
  // Create tables
  /////////////////////////////////////////////////////////

  // Clean up
  delete tpcc_database;
  tpcc_database = nullptr;
  warehouse_table = nullptr;
  district_table = nullptr;
  item_table = nullptr;
  customer_table = nullptr;
  history_table = nullptr;
  stock_table = nullptr;
  new_order_table = nullptr;
  order_line_table = nullptr;

  auto& manager = catalog::Manager::GetInstance();
  tpcc_database = new storage::Database(tpcc_database_oid);
  manager.AddDatabase(tpcc_database);

}

void LoadTPCCDatabase() {
  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  txn_manager.CommitTransaction(txn);
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
