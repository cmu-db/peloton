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

#include "backend/benchmark/ycsb/ycsb_loader.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>

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
namespace ycsb {

storage::DataTable* ycsb_table;

static const oid_t ycsb_field_length = 100;

void CreateUserTable() {
  const oid_t col_count = state.column_count + 1;
  const bool is_inlined = true;

  // Create schema first
  std::vector<catalog::Column> columns;

  auto column =
      catalog::Column(VALUE_TYPE_VARCHAR, ycsb_field_length,
                      "YCSB_KEY", is_inlined);
  columns.push_back(column);

  for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "FIELD" + std::to_string(col_itr), is_inlined);
    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("USERTABLE");

  /////////////////////////////////////////////////////////
  // Create table.
  /////////////////////////////////////////////////////////

  // Clean up
  delete ycsb_table;

  bool own_schema = true;
  bool adapt_table = false;
  ycsb_table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  // Primary index on user key
  std::vector<oid_t> key_attrs;

  auto tuple_schema = ycsb_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "primary_index", 1000, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  ycsb_table->AddIndex(pkey_index);

}

void LoadUserTable() {
  const oid_t col_count = state.column_count + 1;
  const int tuple_count = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;

  auto table_schema = ycsb_table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {

    storage::Tuple tuple(table_schema, allocate);
    auto key_value = ValueFactory::GetIntegerValue(rowid);
    auto field_value = ValueFactory::GetStringValue(std::to_string(rowid));

    tuple.SetValue(0, key_value, nullptr);
    for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
      tuple.SetValue(col_itr, field_value, pool.get());
    }

    ItemPointer tuple_slot_id = ycsb_table->InsertTuple(txn, &tuple);
    assert(tuple_slot_id.block != INVALID_OID);
    assert(tuple_slot_id.offset != INVALID_OID);
    txn->RecordInsert(tuple_slot_id);
  }

  txn_manager.CommitTransaction(txn);

}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
