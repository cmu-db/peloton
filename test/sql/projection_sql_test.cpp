//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_sql_test.cpp
//
// Identification: test/sql/projection_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class ProjectionSQLTests : public PelotonTest {};

TEST_F(ProjectionSQLTests, ProjectionSQLTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 10, 100);");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // test small int
  TestingSQLUtil::ExecuteSQLQuery("SELECT a*5+b, -1+c, 6, a from test", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  // Check the return value
  EXPECT_EQ(result[0].second[0], '1');
  EXPECT_EQ(result[0].second[1], '5');
  EXPECT_EQ(result[1].second[0], '9');
  EXPECT_EQ(result[1].second[1], '9');
  EXPECT_EQ(result[2].second[0], '6');
  EXPECT_EQ(result[3].second[0], '1');

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
