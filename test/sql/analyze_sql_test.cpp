//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_sql_test.cpp
//
// Identification: test/sql/analyze_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "catalog/column_stats_catalog.h"
#include "common/harness.h"
#include "common/internal_types.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "optimizer/stats/stats_storage.h"
#include "planner/create_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class AnalyzeSQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 22, 333, 'abc');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 11, 222, 'abcd');");
}

TEST_F(AnalyzeSQLTests, AnalyzeAllTablesTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ResultType result = TestingSQLUtil::ExecuteSQLQuery("ANALYZE;");
  EXPECT_EQ(result, peloton::ResultType::SUCCESS);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(AnalyzeSQLTests, AnalyzeSingleTableTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  // Execute analyze
  ResultType result = TestingSQLUtil::ExecuteSQLQuery("ANALYZE test;");
  EXPECT_EQ(result, peloton::ResultType::SUCCESS);

  // Check stats information in catalog
  txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  storage::DataTable *db_column_stats_collector_table =
      catalog->GetTableWithName(CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME,
                                COLUMN_STATS_CATALOG_NAME, txn);
  EXPECT_NE(db_column_stats_collector_table, nullptr);
  EXPECT_EQ(db_column_stats_collector_table->GetTupleCount(), 4);

  // Free the database
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(AnalyzeSQLTests, AnalyzeTableWithColumnsTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateAndLoadTable();

  ResultType result = TestingSQLUtil::ExecuteSQLQuery("ANALYZE test (a);");
  EXPECT_EQ(result, peloton::ResultType::SUCCESS);

  // Free the database
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
