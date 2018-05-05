//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_suggestion_util.cpp
//
// Identification: test/brain/testing_index_suggestion_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/testing_index_suggestion_util.h"
#include "brain/what_if_index.h"
#include "common/harness.h"
#include "optimizer/stats/stats_storage.h"
#include "sql/testing_sql_util.h"
#include "planner/index_scan_plan.h"

namespace peloton {

namespace test {

namespace index_suggestion {

/**
 * Creates a database.
 * @param db_name
 */
TestingIndexSuggestionUtil::TestingIndexSuggestionUtil(std::string db_name)
    : database_name_(db_name) {
  srand(time(NULL));
  CreateDatabase();
}

/**
 * Drops all tables and the database.
 */
TestingIndexSuggestionUtil::~TestingIndexSuggestionUtil() {
  for (auto it = tables_created_.begin(); it != tables_created_.end(); it++) {
    DropTable(it->first);
  }
  DropDatabase();
}

/**
 * Creates a new table and inserts specified number of tuples.
 * @param table_name
 * @param schema schema of the table to be created
 * @param num_tuples number of tuples to be inserted with random values.
 */
void TestingIndexSuggestionUtil::CreateAndInsertIntoTable(
    std::string table_name, TableSchema schema, long num_tuples) {
  // Create table.
  std::ostringstream s_stream;
  s_stream << "CREATE TABLE " << table_name << " (";
  for (auto i = 0UL; i < schema.cols.size(); i++) {
    s_stream << schema.cols[i].first;
    s_stream << " ";
    switch (schema.cols[i].second) {
      case FLOAT:
        s_stream << "VARCHAR";
        break;
      case INTEGER:
        s_stream << "INT";
        break;
      case STRING:
        s_stream << "STR";
        break;
      default:
        PELOTON_ASSERT(false);
    }
    if (i < (schema.cols.size() - 1)) {
      s_stream << ", ";
    }
  }
  s_stream << ");";
  TestingSQLUtil::ExecuteSQLQuery(s_stream.str());

  // Insert tuples into table
  for (int i = 0; i < num_tuples; i++) {
    std::ostringstream oss;
    oss << "INSERT INTO " << table_name << " VALUES (";
    for (auto i = 0UL; i < schema.cols.size(); i++) {
      auto type = schema.cols[i].second;
      switch (type) {
        case INTEGER:
          oss << rand() % 1000;
          break;
        case FLOAT:
          oss << rand() * 0.01;
        case STRING:
          oss << "str" << rand() % 1000;
          break;
        default:
          PELOTON_ASSERT(false);
      }
      if (i < (schema.cols.size() - 1)) {
        oss << ", ";
      }
    }
    oss << ");";
    TestingSQLUtil::ExecuteSQLQuery(oss.str());
  }
  GenerateTableStats();
}

/**
 * Generate stats for all the tables in the system.
 */
void TestingIndexSuggestionUtil::GenerateTableStats() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  optimizer::StatsStorage *stats_storage =
      optimizer::StatsStorage::GetInstance();
  ResultType result = stats_storage->AnalyzeStatsForAllTables(txn);
  PELOTON_ASSERT(result == ResultType::SUCCESS);
  (void)result;
  txn_manager.CommitTransaction(txn);
}

/**
 * Factory method to create a hypothetical index object. The returned object can
 * be used
 * in the catalog or catalog cache.
 * @param table_name
 * @param index_col_names
 * @return
 */
std::shared_ptr<brain::IndexObject>
TestingIndexSuggestionUtil::CreateHypotheticalIndex(
    std::string table_name, std::vector<std::string> index_col_names) {
  // We need transaction to get table object.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Get the existing table so that we can find its oid and the cols oids.
  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name_, table_name, txn);
  auto col_obj_pairs = table_object->GetColumnObjects();

  std::vector<oid_t> col_ids;
  auto database_oid = table_object->GetDatabaseOid();
  auto table_oid = table_object->GetTableOid();

  // Find the column oids.
  for (auto it = col_obj_pairs.begin(); it != col_obj_pairs.end(); it++) {
    LOG_DEBUG("Table id: %d, Column id: %d, Offset: %d, Name: %s",
              it->second->GetTableOid(), it->second->GetColumnId(),
              it->second->GetColumnOffset(),
              it->second->GetColumnName().c_str());
    for (auto col_name : index_col_names) {
      if (col_name == it->second->GetColumnName()) {
        col_ids.push_back(it->second->GetColumnId());
      }
    }
  }
  PELOTON_ASSERT(col_ids.size() == index_col_names.size());

  auto obj_ptr = new brain::IndexObject(database_oid, table_oid, col_ids);
  auto index_obj = std::shared_ptr<brain::IndexObject>(obj_ptr);

  txn_manager.CommitTransaction(txn);
  return index_obj;
}

/**
 * Create the database
 */
void TestingIndexSuggestionUtil::CreateDatabase() {
  std::string create_db_str = "CREATE DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_db_str);
}

/**
 * Drop the database
 */
void TestingIndexSuggestionUtil::DropDatabase() {
  std::string create_str = "DROP DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

/**
 * Drop the table
 */
void TestingIndexSuggestionUtil::DropTable(std::string table_name) {
  std::string create_str = "DROP TABLE " + table_name + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

} // namespace index_suggestion
} // namespace test
} // namespace peloton
