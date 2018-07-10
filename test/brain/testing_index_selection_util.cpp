//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_selection_util.cpp
//
// Identification: test/brain/testing_index_selection_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/testing_index_selection_util.h"
#include "brain/what_if_index.h"
#include "common/harness.h"
#include "optimizer/stats/stats_storage.h"
#include "sql/testing_sql_util.h"
#include "planner/index_scan_plan.h"

namespace peloton {

namespace test {

namespace index_selection {

TestingIndexSelectionUtil::TestingIndexSelectionUtil(std::string db_name)
    : database_name_(db_name) {
  srand(time(NULL));
  CreateDatabase();
}

TestingIndexSelectionUtil::~TestingIndexSelectionUtil() {
  for (auto it = tables_created_.begin(); it != tables_created_.end(); it++) {
    DropTable(it->first);
  }
  DropDatabase();
}

std::pair<std::vector<TableSchema>, std::vector<std::string>>
TestingIndexSelectionUtil::GetQueryStringsWorkload(
    QueryStringsWorkloadType type) {
  std::vector<std::string> query_strs;
  std::vector<TableSchema> table_schemas;
  std::string table_name;
  // Procedure to add a new workload:
  // 1. Create all the table schemas required for the workload queries.
  // 2. Create all the required workload query strings.
  // Note on Naming of workloads: <num_tables>Table<num_accessed_cols>ColW
  switch (type) {
    case QueryStringsWorkloadType::SingleTableNoop: {
      table_name = "dummy0";
      table_schemas.emplace_back(
          table_name,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGERPKEY},
              {"c", TupleValueType::INTEGER}});
      // This query string is not actually executed - only used for testing
      // add/drop candidates
      query_strs.push_back("UPDATE dummy0 SET a = 0 WHERE b = 1 AND c = 2");
      break;
    }
    case QueryStringsWorkloadType::SingleTableTwoColW1: {
      table_name = "dummy1";
      table_schemas.emplace_back(
          table_name,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGER},
              {"c", TupleValueType::INTEGER},
              {"d", TupleValueType::INTEGER}});
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE a = 160 and a = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE c = 190 and c = 250");
      query_strs.push_back("SELECT a, b, c FROM " + table_name +
          " WHERE a = 190 and c = 250");
      break;
    }
    case QueryStringsWorkloadType::SingleTableTwoColW2: {
      table_name = "dummy2";
      table_schemas.emplace_back(
          table_name,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGER},
              {"c", TupleValueType::INTEGER},
              {"d", TupleValueType::INTEGER}});
      query_strs.push_back("SELECT * FROM " + table_name + " WHERE a = 160");
      query_strs.push_back("SELECT * FROM " + table_name + " WHERE b = 190");
      query_strs.push_back("SELECT * FROM " + table_name + " WHERE b = 81");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE a = 190 and b = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE a = 190 and b = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 190 and a = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 190 and c = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 190 and c = 250");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE a = 190 and c = 250");
      break;
    }
    case QueryStringsWorkloadType::SingleTableThreeColW: {
      table_name = "dummy3";
      table_schemas.emplace_back(
          table_name,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGER},
              {"c", TupleValueType::INTEGER},
              {"d", TupleValueType::INTEGER},
              {"e", TupleValueType::INTEGER},
              {"f", TupleValueType::INTEGER},
              {"g", TupleValueType::INTEGER}});
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE a = 160 and b = 199 and c = 1009");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 190 and a = 677 and c = 987");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 81 and c = 123 and a = 122");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 81 and c = 123 and d = 122");
      query_strs.push_back("SELECT * FROM " + table_name + " WHERE b = 81");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE b = 81 and c = 12");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE d = 81 and e = 123 and f = 122");
      query_strs.push_back("SELECT * FROM " + table_name + " WHERE d = 81");
      query_strs.push_back("SELECT * FROM " + table_name +
          " WHERE d = 81 and e = 12");
      break;
    }
    case QueryStringsWorkloadType::MultiTableNoop: {
      std::string table_name_1 = "dummy1";
      table_schemas.emplace_back(
          table_name_1,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGERPKEY},
              {"c", TupleValueType::INTEGER}});
      std::string table_name_2 = "dummy2";
      table_schemas.emplace_back(
          table_name_2,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGER},
              {"c", TupleValueType::INTEGER}});
      std::string table_name_3 = "dummy3";
      table_schemas.emplace_back(
          table_name_3,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"a", TupleValueType::INTEGER},
              {"b", TupleValueType::INTEGER},
              {"c", TupleValueType::INTEGER}});
      // No workload
      break;
    }
    case QueryStringsWorkloadType::MultiTableMultiColW: {
      std::string table_name_1 = "d_student";
      table_schemas.emplace_back(
          table_name_1,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"name", TupleValueType::STRING},
              {"gpa", TupleValueType::INTEGER},
              {"id", TupleValueType::INTEGER},
              {"cgpa", TupleValueType::INTEGER}});
      std::string table_name_2 = "d_college";
      table_schemas.emplace_back(
          table_name_2,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"name", TupleValueType::STRING},
              {"city", TupleValueType::STRING},
              {"county", TupleValueType::STRING},
              {"state", TupleValueType::STRING},
              {"country", TupleValueType::STRING},
              {"enrolment", TupleValueType::INTEGER}});
      std::string table_name_3 = "d_course";
      table_schemas.emplace_back(
          table_name_3,
          std::initializer_list<std::pair<std::string, TupleValueType>>{
              {"name", TupleValueType::STRING},
              {"id", TupleValueType::INTEGER}});
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE name = 'vamshi' and id = 40");
      query_strs.push_back("SELECT * FROM " + table_name_1 + " WHERE id = 100");
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE name = 'siva' and id = 50");
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE name = 'priyatham' and id = 60");
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE id = 69 and name = 'vamshi'");
      query_strs.push_back("SELECT * FROM " + table_name_1 + " WHERE id = 4");
      query_strs.push_back("SELECT * FROM " + table_name_1 + " WHERE id = 10");
      query_strs.push_back("SELECT cgpa FROM " + table_name_1 +
                           " WHERE name = 'vam'");
      query_strs.push_back("SELECT name FROM " + table_name_1 +
                           " WHERE cgpa = 3");
      query_strs.push_back("SELECT name FROM " + table_name_1 +
                           " WHERE cgpa = 9 and gpa = 9");
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE cgpa = 9 and gpa = 9 and name = 'vam'");
      query_strs.push_back("SELECT * FROM " + table_name_1 +
                           " WHERE gpa = 9 and name = 'vam' and cgpa = 9");
      query_strs.push_back("SELECT country FROM " + table_name_2 +
                           " WHERE name = 'cmu'");
      query_strs.push_back("UPDATE " + table_name_2 +
                           " set name = 'cmu' where country = 'usa'");
      query_strs.push_back("UPDATE " + table_name_2 +
                           " set name = 'berkeley' where country = 'usa'");
      query_strs.push_back("DELETE FROM " + table_name_1 +
                           " where name = 'vam'");
      query_strs.push_back("DELETE FROM " + table_name_2 +
                           " where name = 'vam'");
      query_strs.push_back("DELETE FROM " + table_name_1 + " where id = 1");
      query_strs.push_back(
          "SELECT * FROM d_student s inner join d_college c on s.name = "
          "c.name inner join d_course co on c.name = co.name");
      // The below 2(especially last one is prohibitively expensive)
      // Unable to understand whether What-If is correctly measuring - since
      // difference is minimal with or without indexes :/
//      query_strs.push_back(
//          "SELECT * FROM d_student join d_college on d_student.name = "
//          "d_college.name");
//      query_strs.push_back("SELECT * FROM " + table_name_1 + " t1 ," +
//                           table_name_2 + " t2 where t1.name = 'vam'");
      break;
    }
    default:
      PELOTON_ASSERT(false);
  }
  return std::make_pair(table_schemas, query_strs);
}

std::pair<std::vector<TableSchema>, std::vector<std::string>>
TestingIndexSelectionUtil::GetCyclicWorkload(std::vector<QueryStringsWorkloadType> workload_types,
                                             int num_cycles) {
  // Using table names to prevent duplication
  std::set<std::string> schemas_processed;
  std::vector<std::string> query_strs;
  std::vector<TableSchema> table_schemas;
  for(const auto &w_type: workload_types) {
    auto config = GetQueryStringsWorkload(w_type);
    auto config_schemas = config.first;
    for(const auto &table_schema: config_schemas) {
      if(schemas_processed.find(table_schema.table_name) == schemas_processed.end()) {
        schemas_processed.insert(table_schema.table_name);
        table_schemas.push_back(table_schema);
      }
    }
    auto config_queries = config.second;
    query_strs.insert(query_strs.end(), config_queries.begin(), config_queries.end());
  }
  for(int i = 0; i < num_cycles - 1; i++) {
    query_strs.insert(query_strs.end(), query_strs.begin(), query_strs.end());
  }
  return std::make_pair(table_schemas, query_strs);
}

// Creates a new table with the provided schema.
void TestingIndexSelectionUtil::CreateTable(TableSchema schema) {
  // Create table.
  std::ostringstream s_stream;
  s_stream << "CREATE TABLE " << schema.table_name << " (";
  for (auto i = 0UL; i < schema.cols.size(); i++) {
    s_stream << schema.cols[i].first;
    s_stream << " ";
    switch (schema.cols[i].second) {
      case TupleValueType::FLOAT:
        s_stream << "FLOAT";
        break;
      case TupleValueType::INTEGER:
        s_stream << "INT";
        break;
      case TupleValueType::STRING:
        s_stream << "VARCHAR(30)";
        break;
      case TupleValueType::INTEGERPKEY:
        s_stream << "INT PRIMARY KEY";
        break;
      default:
        PELOTON_ASSERT(false);
    }
    if (i < (schema.cols.size() - 1)) {
      s_stream << ", ";
    }
  }
  s_stream << ");";
  LOG_DEBUG("Create table: %s", s_stream.str().c_str());
  TestingSQLUtil::ExecuteSQLQuery(s_stream.str());
}

// Inserts specified number of tuples into the table with random values.
void TestingIndexSelectionUtil::InsertIntoTable(TableSchema schema,
                                                long num_tuples) {
  // Insert tuples into table
  for (int i = 0; i < num_tuples; i++) {
    std::ostringstream oss;
    oss << "INSERT INTO " << schema.table_name << " VALUES (";
    for (auto col = 0UL; col < schema.cols.size(); col++) {
      auto type = schema.cols[col].second;
      switch (type) {
        case TupleValueType::INTEGER:
          oss << rand() % 1000;
          break;
        case TupleValueType::INTEGERPKEY:
          oss << rand() % 1000;
          break;
        case TupleValueType::FLOAT:
          oss << (float)(rand() % 100);
          break;
        case TupleValueType::STRING:
          oss << "'str" << rand() % RAND_MAX << "'";
          break;
        default:
          PELOTON_ASSERT(false);
      }
      if (col < (schema.cols.size() - 1)) {
        oss << ", ";
      }
    }
    oss << ");";
    LOG_TRACE("Inserting: %s", oss.str().c_str());
    TestingSQLUtil::ExecuteSQLQuery(oss.str());
  }
  GenerateTableStats();
}

void TestingIndexSelectionUtil::GenerateTableStats() {
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
TestingIndexSelectionUtil::CreateHypotheticalIndex(
    std::string table_name, std::vector<std::string> index_col_names,
    brain::IndexSelection *is) {
  // We need transaction to get table object.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Get the existing table so that we can find its oid and the cols oids.
  auto table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn,
      database_name_, "public", table_name);
  auto col_obj_pairs = table_object->GetColumnCatalogEntries();

  std::vector<oid_t> col_ids;
  auto database_oid = table_object->GetDatabaseOid();
  auto table_oid = table_object->GetTableOid();

  // Find the column oids.
  for (auto col_name : index_col_names) {
    for (auto it = col_obj_pairs.begin(); it != col_obj_pairs.end(); it++) {
      LOG_DEBUG("Table id: %d, Column id: %d, Offset: %d, Name: %s",
                it->second->GetTableOid(), it->second->GetColumnId(),
                it->second->GetColumnOffset(),
                it->second->GetColumnName().c_str());
      if (col_name == it->second->GetColumnName()) {
        col_ids.push_back(it->second->GetColumnId());
      }
    }
  }
  PELOTON_ASSERT(col_ids.size() == index_col_names.size());

  std::shared_ptr<brain::HypotheticalIndexObject> index_obj;

  if (is == nullptr) {
    auto obj_ptr =
        new brain::HypotheticalIndexObject(database_oid, table_oid, col_ids);
    index_obj = std::shared_ptr<brain::HypotheticalIndexObject>(obj_ptr);
  } else {
    auto obj = brain::HypotheticalIndexObject(database_oid, table_oid, col_ids);
    index_obj = is->AddConfigurationToPool(obj);
  }

  txn_manager.CommitTransaction(txn);
  return index_obj;
}

void TestingIndexSelectionUtil::CreateDatabase() {
  std::string create_db_str = "CREATE DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_db_str);
}

void TestingIndexSelectionUtil::DropDatabase() {
  std::string create_str = "DROP DATABASE " + database_name_ + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

void TestingIndexSelectionUtil::DropTable(std::string table_name) {
  std::string create_str = "DROP TABLE " + table_name + ";";
  TestingSQLUtil::ExecuteSQLQuery(create_str);
}

void TestingIndexSelectionUtil::CreateIndex(std::shared_ptr<brain::HypotheticalIndexObject> index_obj) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  std::string table_name =
      catalog->GetTableCatalogEntry(txn, index_obj->db_oid, index_obj->table_oid)->GetTableName();

  catalog->CreateIndex(txn, database_name_, DEFAULT_SCHEMA_NAME, table_name,
                       index_obj->ToString(), index_obj->column_oids, false,
                       IndexType::BWTREE);
  txn_manager.CommitTransaction(txn);
}

double TestingIndexSelectionUtil::WhatIfIndexCost(std::string query,
                                                   brain::IndexConfiguration &config,
                                                   std::string database_name) {
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser::PostgresParser::ParseSQLString(query));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, database_name));

  // Get the first statement.
  auto sql_statement = std::shared_ptr<parser::SQLStatement>(
      stmt_list->PassOutStatement(0));

  binder->BindNameToNode(sql_statement.get());
  auto cost = brain::WhatIfIndex::GetCostAndBestPlanTree(sql_statement, config,
                                                         database_name, txn)->cost;
  txn_manager.CommitTransaction(txn);
  return cost;
}

}  // namespace index_selection
}  // namespace test
}  // namespace peloton
