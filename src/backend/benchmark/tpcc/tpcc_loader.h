//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/tpcc/tpcc_configuration.h"

namespace peloton {
namespace storage{
  class Database;
  class DataTable;
}

namespace benchmark {
namespace tpcc {

extern configuration state;

extern storage::Database* tpcc_database;
extern storage::DataTable* warehouse_table;
extern storage::DataTable* district_table;
extern storage::DataTable* item_table;
extern storage::DataTable* customer_table;
extern storage::DataTable* history_table;
extern storage::DataTable* stock_table;
extern storage::DataTable* orders_table;
extern storage::DataTable* new_order_table;
extern storage::DataTable* order_line_table;

void CreateTPCCDatabase();

void LoadTPCCDatabase();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
