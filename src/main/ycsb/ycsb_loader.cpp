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
#include "concurrency/transaction_context.h"
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
// extern peloton::LoggingType peloton_logging_mode;

namespace peloton {
namespace benchmark {
namespace ycsb {

storage::Database *ycsb_database = nullptr;

storage::DataTable *user_table = nullptr;

void CreateYCSBDatabase() {
  const oid_t col_count = state.column_count + 1;
  const bool is_inlined = false;

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
  // Create schema first
  std::vector<catalog::Column> columns;

  auto column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "YCSB_KEY", is_inlined);
  columns.push_back(column);

  if (state.string_mode == true) {
    for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
        auto column =
            catalog::Column(type::TypeId::VARCHAR, 100,
                            "FIELD" + std::to_string(col_itr), is_inlined);
        columns.push_back(column);
    }
  } else {
    for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
        auto column =
            catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                            "FIELD" + std::to_string(col_itr), is_inlined);
        columns.push_back(column);
    }
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
    ycsb_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
    tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  user_table->AddIndex(pkey_index);
}

void LoadYCSBRows(const int begin_rowid, const int end_rowid) {
  const oid_t col_count = state.column_count + 1;

  // Pick the user table
  auto table_schema = user_table->GetSchema();

  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  for (int rowid = begin_rowid; rowid < end_rowid; rowid++) {
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table_schema, allocate));

    auto primary_key_value = type::ValueFactory::GetIntegerValue(rowid);
    tuple->SetValue(0, primary_key_value, nullptr);


    if (state.string_mode == true) {
      auto key_value = type::ValueFactory::GetVarcharValue(std::string(100, 'z'));
      for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
        tuple->SetValue(col_itr, key_value, pool.get());
      }
    } else {
      auto key_value = type::ValueFactory::GetIntegerValue(rowid);
      for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
        tuple->SetValue(col_itr, key_value, nullptr);
      }
    }

    planner::InsertPlan node(user_table, std::move(tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction(txn);
}

void LoadYCSBDatabase() {

  std::chrono::steady_clock::time_point start_time;
  start_time = std::chrono::steady_clock::now();

  const int tuple_count = state.scale_factor * 1000;
  int row_per_thread = tuple_count / state.loader_count;
  
  std::vector<std::unique_ptr<std::thread>> load_threads(state.loader_count);

  for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
    int begin_rowid = row_per_thread * thread_id;
    int end_rowid = row_per_thread * (thread_id + 1);
    load_threads[thread_id].reset(new std::thread(LoadYCSBRows, begin_rowid, end_rowid));
  }
  
  int thread_id = state.loader_count - 1;
  int begin_rowid = row_per_thread * thread_id;
  int end_rowid = tuple_count;
  load_threads[thread_id].reset(new std::thread(LoadYCSBRows, begin_rowid, end_rowid));

  for (int thread_id = 0; thread_id < state.loader_count; ++thread_id) {
    load_threads[thread_id]->join();
  }

  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  LOG_INFO("database table loading time = %lf ms", diff);

  LOG_INFO("%sTABLE SIZES%s", peloton::GETINFO_HALF_THICK_LINE.c_str(), peloton::GETINFO_HALF_THICK_LINE.c_str());
  LOG_INFO("user count = %lu", user_table->GetTupleCount());

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
