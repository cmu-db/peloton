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

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
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
  try {
    table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                              "test");
  } catch (CatalogException &e) {
    table = nullptr;
  }
  EXPECT_NE(table, nullptr);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Insert and query from that table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 10);", result,
                                  tuple_descriptor, rows_affected);
  TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                  tuple_descriptor, rows_affected);
  EXPECT_EQ(result[0].second[0], '1');

  // Drop the table
  EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery("DROP TABLE test;"),
            ResultType::SUCCESS);

  // Query from the dropped table
  result.clear();
  TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                  tuple_descriptor, rows_affected);
  EXPECT_EQ(result.empty(), true);

  // Check the table does not exist
  try {
    table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                              "test");
  } catch (CatalogException &e) {
    table = nullptr;
  }
  EXPECT_EQ(table, nullptr);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
