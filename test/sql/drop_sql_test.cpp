//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_sql_test.cpp
//
// Identification: test/sql/drop_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/index_catalog.h"
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class DropSQLTests : public PelotonTest {};

TEST_F(DropSQLTests, DropTableTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");

  // Check the table in catalog
  storage::DataTable *table;
  txn = txn_manager.BeginTransaction();
  try {
    table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                              "test", txn);
  } catch (CatalogException &e) {
    table = nullptr;
  }
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(table, nullptr);

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Insert and query from that table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 10);", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0][0], '1');

  // Drop the table
  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery("DROP TABLE test;"),
            ResultType::SUCCESS);

  // Query from the dropped table
  result.clear();
  TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result.empty(), true);

  // Check the table does not exist
  txn = txn_manager.BeginTransaction();
  try {
    table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                              "test", txn);
  } catch (CatalogException &e) {
    txn_manager.CommitTransaction(txn);
    table = nullptr;
  }
  EXPECT_EQ(table, nullptr);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DropSQLTests, DropIndexTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT);");

  // Create a Index
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX idx ON test(a);");

  // Check if the index is in catalog
  std::shared_ptr<catalog::IndexCatalogObject> index;
  txn = txn_manager.BeginTransaction();
  try {
    index = catalog::IndexCatalog::GetInstance()->GetIndexObject("idx", txn);

  } catch (CatalogException &e) {
    index = nullptr;
  }
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(index, nullptr);

  // Drop the index
  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery("DROP INDEX idx;"),
            ResultType::SUCCESS);

  // Check if index is not in catalog
  txn = txn_manager.BeginTransaction();
  index = catalog::IndexCatalog::GetInstance()->GetIndexObject("idx", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(index, nullptr);

  // Free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton