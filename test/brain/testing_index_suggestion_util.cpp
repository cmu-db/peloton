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

TestingIndexSuggestionUtil::TestingIndexSuggestionUtil(std::string db_name)
    : database_name_(db_name) {
  srand(time(NULL));
  CreateDatabase();
}

TestingIndexSuggestionUtil::~TestingIndexSuggestionUtil() {
  for (auto it = tables_created_.begin(); it != tables_created_.end(); it++) {
    DropTable(it->first);
  }
  DropDatabase();
}

// Creates a new table with the provided schema.
void TestingIndexSuggestionUtil::CreateTable(std::string table_name,
                                             TableSchema schema) {
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
}

// Check whether the given indexes are the same as the expected ones
bool TestingIndexSuggestionUtil::CheckIndexes(
    brain::IndexConfiguration chosen_indexes,
    std::set<std::set<oid_t>> expected_indexes) {
  if(chosen_indexes.GetIndexCount() != expected_indexes.size()) return false;

  for (auto expected_columns : expected_indexes) {
    bool found = false;
    for (auto chosen_index : chosen_indexes.GetIndexes()) {
      if(chosen_index->column_oids == expected_columns) {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }
  return true;
}

// Inserts specified number of tuples into the table with random values.
void TestingIndexSuggestionUtil::InsertIntoTable(std::string table_name,
                                                 TableSchema schema,
                                                 long num_tuples) {
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

// Factory method
// Returns a what-if index on the columns at the given
// offset of the table.
std::shared_ptr<brain::HypotheticalIndexObject>
TestingIndexSuggestionUtil::CreateHypotheticalIndex(
    std::string table_name, std::vector<std::string> index_col_names) {
  // We need transaction to get table object.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Get the existing table so that we can find its oid and the cols oids.
  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name_, DEFUALT_SCHEMA_NAME, table_name, txn);
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

  auto obj_ptr =
      new brain::HypotheticalIndexObject(database_oid, table_oid, col_ids);
  auto index_obj = std::shared_ptr<brain::HypotheticalIndexObject>(obj_ptr);

  txn_manager.CommitTransaction(txn);
  return index_obj;
}

void TestingIndexSuggestionUtil::CreateDatabase() {
  std::string create_db_str = "CREATE DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_db_str);
}

void TestingIndexSuggestionUtil::DropDatabase() {
  std::string create_str = "DROP DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

void TestingIndexSuggestionUtil::DropTable(std::string table_name) {
  std::string create_str = "DROP TABLE " + table_name + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

}  // namespace index_suggestion
}  // namespace test
}  // namespace peloton
