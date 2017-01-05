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

#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

#include "sql/sql_tests_util.h"

namespace peloton {
namespace test {

class DropSQLTests : public PelotonTest {};

TEST_F(DropSQLTests, DropSQLTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create a table first
  SQLTestsUtil::ExecuteSQLQuery("CREATE TABLE test(a INT PRIMARY KEY, b INT);");

  // Check the table in catalog
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  std::vector<ResultType> result;
  std::vector<FieldInfoType> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Insert and query from that table
  SQLTestsUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 10, 100);", result,
                                tuple_descriptor, rows_affected, error_message);
  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test;", result, tuple_descriptor,
                                rows_affected, error_message);
  EXPECT_EQ(result[0].second[0], '1');

  // Drop the table
  SQLTestsUtil::ExecuteSQLQuery("DROP TABLE test;");

  // Query from the dropped table
  SQLTestsUtil::ExecuteSQLQuery("SELECT * FROM test;", result, tuple_descriptor,
                                rows_affected, error_message);
  EXPECT_EQ(result.empty(), true);

  // Check the table does not exist
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            0);

  // free the database just created
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}

}  // namespace test
}  // namespace peloton
