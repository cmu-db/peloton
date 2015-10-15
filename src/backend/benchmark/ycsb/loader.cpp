//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// loader.cpp
//
// Identification: benchmark/ycsb/loader.cpp
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

#include "backend/benchmark/ycsb/loader.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/index/index_factory.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

storage::DataTable *user_table;

std::vector<catalog::Column> GetColumns(){
  const oid_t col_count = state.column_count;
  bool is_inlined;

  // Create schema first
  std::vector<catalog::Column> columns;

  is_inlined = true;
  auto key_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                                    "YCSB_KEY", is_inlined);
  columns.push_back(key_column);

  is_inlined = false;
  for(oid_t col_itr = 0 ; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_VARCHAR, state.value_length,
                        "FIELD" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  return columns;
}

static void CreateTable() {

  auto columns = GetColumns();
  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("USERTABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  user_table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      state.tuples_per_tilegroup,
      own_schema, adapt_table);

}

static void LoadTable() {

  const oid_t col_count = state.column_count;
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;

  auto table_schema = user_table->GetSchema();
  std::string string_value('.', state.value_length);

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {
    storage::Tuple tuple(table_schema, allocate);

    auto key_value = ValueFactory::GetIntegerValue(rowid);
    tuple.SetValue(0, key_value);

    for(oid_t col_itr = 1 ; col_itr <= col_count; col_itr++) {
      auto value = ValueFactory::GetStringValue(string_value);
      tuple.SetValue(col_itr, value);
    }

    ItemPointer tuple_slot_id = user_table->InsertTuple(txn, &tuple);
    assert(tuple_slot_id.block != INVALID_OID);
    assert(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction(txn);

}

void CreateAndLoadTable(LayoutType layout_type) {

  // Initialize settings
  peloton_layout = layout_type;

  CreateTable();

  LoadTable();
}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
