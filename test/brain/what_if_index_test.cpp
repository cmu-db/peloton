//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tensorflow_test.cpp
//
// Identification: test/brain/tensorflow_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "catalog/index_catalog.h"
#include "brain/what_if_index.h"
#include "sql/testing_sql_util.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/table_stats.h"

namespace peloton {

using namespace brain;
using namespace catalog;

namespace test {

using namespace optimizer;

//===--------------------------------------------------------------------===//
// WhatIfIndex Tests
//===--------------------------------------------------------------------===//

class WhatIfIndexTests : public PelotonTest {
private:
  std::string database_name;
public:

  WhatIfIndexTests() {
    database_name = DEFAULT_DB_NAME;
  }

  // Create a new database
  void CreateDatabase() {
    // Create a new database.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(database_name, txn);
    txn_manager.CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(std::string table_name) {
    std::string create_str = "CREATE TABLE " + table_name + "(a INT, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  // Inserts a given number of tuples with increasing values into the table.
  void InsertIntoTable(std::string table_name, int no_of_tuples) {
    // Insert tuples into table
    for (int i=0; i<no_of_tuples; i++) {
      std::ostringstream oss;
      oss << "INSERT INTO "<< table_name <<" VALUES (" << i << ","
          << i + 1 << "," << i + 2 << ");";
      TestingSQLUtil::ExecuteSQLQuery(oss.str());
    }
  }

  // Generates table stats to perform what-if index queries.
  void GenerateTableStats() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    StatsStorage *stats_storage = StatsStorage::GetInstance();
    ResultType result = stats_storage->AnalyzeStatsForAllTables(txn);
    assert(result == ResultType::SUCCESS);
    txn_manager.CommitTransaction(txn);
  }

  // Create a what-if single column index on a column at the given
  // offset of the table.
  std::shared_ptr<IndexCatalogObject> CreateHypotheticalSingleIndex(
    std::string table_name, oid_t col_offset) {

    // We need transaction to get table object.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Get the existing table so that we can find its oid and the cols oids.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, table_name, txn);

    std::vector<oid_t> cols;
    auto col_obj_pairs = table_object->GetColumnObjects();

    // Find the column oid.
    for (auto it = col_obj_pairs.begin(); it != col_obj_pairs.end(); it++) {
      LOG_DEBUG("Table id: %d, Column id: %d, Offset: %d, Name: %s", it->second->GetTableOid(),
               it->second->GetColumnId(), it->second->GetColumnOffset(),
               it->second->GetColumnName().c_str());
      if (it->second->GetColumnId() == col_offset) {
        cols.push_back(it->second->GetColumnId()); // we just need the oid.
        break;
      }
    }
    assert(cols.size() == 1);

    // Give dummy index oid and name.
    std::ostringstream index_name_oss;
    index_name_oss << "index_" << col_offset;

    auto index_obj = std::shared_ptr<IndexCatalogObject> (
      new IndexCatalogObject(col_offset, index_name_oss.str(), table_object->GetTableOid(),
                             IndexType::BWTREE, IndexConstraintType::DEFAULT,
                             false, cols));

    txn_manager.CommitTransaction(txn);
    return index_obj;
  }
};

TEST_F(WhatIfIndexTests, BasicTest) {

  std::string table_name = "dummy_table_whatif";

  CreateDatabase();

  CreateTable(table_name);

  InsertIntoTable(table_name, 1000);

  GenerateTableStats();

  // Form the query.
  std::ostringstream query_str_oss;
  query_str_oss << "SELECT a from " << table_name << " WHERE " <<
                "b < 100 and c < 5;";

  std::vector<std::shared_ptr<IndexCatalogObject>> index_objs;

  std::unique_ptr<parser::SQLStatementList> stmt_list(
    parser::PostgresParser::ParseSQLString(query_str_oss.str()));

  // 1. Get the optimized plan tree without the indexes (sequential scan)
  WhatIfIndex wif;
  auto result = wif.GetCostAndPlanTree(stmt_list, index_objs, DEFAULT_DB_NAME);
  auto cost_without_index = result->cost;
  LOG_INFO("Cost of the query without indexes: %lf", cost_without_index);

  // 2. Get the optimized plan tree with 1 hypothetical indexes (indexes)
  index_objs.push_back(CreateHypotheticalSingleIndex(table_name, 1));

  result = wif.GetCostAndPlanTree(stmt_list, index_objs, DEFAULT_DB_NAME);
  auto cost_with_index_1 = result->cost;
  LOG_INFO("Cost of the query with 1 index: %lf", cost_with_index_1);

  // 3. Get the optimized plan tree with 2 hypothetical indexes (indexes)
  index_objs.push_back(CreateHypotheticalSingleIndex(table_name, 2));

  result = wif.GetCostAndPlanTree(stmt_list, index_objs, DEFAULT_DB_NAME);
  auto cost_with_index_2 = result->cost;
  LOG_INFO("Cost of the query with 2 indexes: %lf", cost_with_index_2);

  EXPECT_LT(cost_with_index_1, cost_without_index);
  EXPECT_LT(cost_with_index_2, cost_without_index);
}

} // namespace test
} // namespace peloton
