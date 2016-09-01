//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.cpp
//
// Identification: src/main/ycsb/ycsb_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>

#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/database.h"

// Logging mode
extern LoggingType peloton_logging_mode;

namespace peloton {
namespace benchmark {
namespace ycsb {

storage::Database *ycsb_database;

storage::DataTable *user_table;

void CreateYCSBDatabase() {
  const oid_t col_count = state.column_count + 1;

  /////////////////////////////////////////////////////////
  // Create tables
  /////////////////////////////////////////////////////////

  // Clean up
  delete ycsb_database;
  ycsb_database = nullptr;
  user_table = nullptr;

  auto catalog = catalog::Catalog::GetInstance();
  ycsb_database = new storage::Database(ycsb_database_oid);
  catalog->AddDatabase(ycsb_database);

  bool own_schema = true;
  bool adapt_table = false;
  const bool is_inlined = false;

  // Create schema first
  std::vector<catalog::Column> columns;

  auto column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
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

  user_table = storage::TableFactory::GetDataTable(
      ycsb_database_oid, user_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  ycsb_database->AddTable(user_table);

  // Primary index on user key
  std::vector<oid_t> key_attrs;

  auto tuple_schema = user_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "primary_index", user_table_pkey_index_oid, user_table_oid,
      ycsb_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
      tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetInstance(index_metadata));
  user_table->AddIndex(pkey_index);
}

void LoadYCSBDatabase() {
  const oid_t col_count = state.column_count + 1;
  const int tuple_count = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;

  // Pick the user table
  auto table_schema = user_table->GetSchema();
  std::string field_raw_value(ycsb_field_length - 1, 'o');

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  int rowid;
  for (rowid = 0; rowid < tuple_count; rowid++) {
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table_schema, allocate));
    auto key_value = ValueFactory::GetIntegerValue(rowid);

    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      tuple->SetValue(col_itr, key_value, nullptr);
    }

    planner::InsertPlan node(user_table, std::move(tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction(txn);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
