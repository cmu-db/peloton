//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_by_sql_test.cpp
//
// Identification: test/sql/group_by_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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

class TruncateTests : public PelotonTest {};

TEST_F(TruncateTests, EmptyTableTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE xxx(a INT PRIMARY KEY, b INT);");
}
TEST_F(TruncateTests, SimpleTruncateTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Create a table first
  // into the table
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE xxx(a INT PRIMARY KEY, b INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO xxx VALUES (3, 4);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO xxx VALUES (1, 2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO xxx VALUES (5, 6);");

  TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "xxx");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;
  std::string expected;

  TestingSQLUtil::ExecuteSQLQuery("TRUNCATE xxx;", result, tuple_descriptor,
                                  rows_affected, error_message);

  // Check the return value
  EXPECT_EQ(3, rows_affected);
  TestingSQLUtil::ExecuteSQLQuery("SELECT COUNT(*) FROM xxx;", result,
                                  tuple_descriptor, rows_affected,
                                  error_message);
  EXPECT_EQ(result[0].second[0], '0');
  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
