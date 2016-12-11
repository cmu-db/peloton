//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_loader.cpp
//
// Identification: src/main/sdbench/sdbench_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/sdbench/sdbench_loader.h"
#include "benchmark/sdbench/sdbench_configuration.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "expression/constant_value_expression.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

std::unique_ptr<storage::DataTable> sdbench_table;

void CreateTable() {
  const oid_t col_count = state.attribute_count + 1;
  const bool is_inlined = true;

  // Create schema first
  std::vector<catalog::Column> columns;

  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(col_itr), is_inlined);

    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("SDBENCHTABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  bool own_schema = true;
  bool adapt_table = true;
  sdbench_table.reset(storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      state.tuples_per_tilegroup, own_schema, adapt_table));
}

void LoadTable() {
  const oid_t col_count = state.attribute_count + 1;
  const int tuple_count = state.scale_factor * state.tuples_per_tilegroup;

  auto table_schema = sdbench_table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple(table_schema, allocate);

    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      auto value = ValueFactory::GetIntegerValue(populate_value);
      tuple.SetValue(col_itr, value, pool.get());
    }

    ItemPointer tuple_slot_id = sdbench_table->InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction();
}

void CreateAndLoadTable(LayoutType layout_type) {
  // Initialize settings
  peloton_layout_mode = layout_type;

  CreateTable();

  LoadTable();
}

void DropIndexes() {
  // Drop index
  sdbench_table->DropIndexes();
}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
