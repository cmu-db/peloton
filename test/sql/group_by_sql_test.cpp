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

class GroupBySQLTests : public PelotonTest {};

void CreateAndLoadTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, d VARCHAR);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1, 22, 333, 'abcd');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (2, 33, 111, 'bcda');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (3, 11, 222, 'bcd');");
}

TEST_F(GroupBySQLTests, EmptyTableTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "create table xxx (id int, name varchar, salary decimal);");
}
TEST_F(GroupBySQLTests, SimpleGroupByTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Create a table first
  // into the table
  TestingSQLUtil::ExecuteSQLQuery(
      "create table xxx (id int, name varchar, salary decimal);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(1, 'Mike', 1000);");
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(2, 'Jane', 2000);");
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(3, 'Tom', 3000);");
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(4, 'Kelly', 4000);");
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(5, 'Lucy', 3000);");
  TestingSQLUtil::ExecuteSQLQuery("insert into xxx values(6, 'Tim', 2000);");
  TestingSQLUtil::ShowTable(DEFAULT_DB_NAME, "test");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  optimizer::SimpleOptimizer optimizer;

  TestingSQLUtil::ExecuteSQLQuery(
      "select count(id), salary from xxx group by salary;", result,
      tuple_descriptor, rows_affected, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_affected);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 6));

  // test: GROUP BY + HAVING
  TestingSQLUtil::ExecuteSQLQuery(
      "select count(id), salary from xxx group by salary having salary>1000;",
      result, tuple_descriptor, rows_affected, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_affected);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 4));

  // test: GROUP BY + ORDER BY
  TestingSQLUtil::ExecuteSQLQuery(
      "select count(id), salary from xxx group by salary order by salary;",
      result, tuple_descriptor, rows_affected, error_message);

  // Check the return value
  EXPECT_EQ(0, rows_affected);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("2", TestingSQLUtil::GetResultValueAsString(result, 4));
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 6));
  EXPECT_EQ("1000", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("2000", TestingSQLUtil::GetResultValueAsString(result, 3));
  EXPECT_EQ("3000", TestingSQLUtil::GetResultValueAsString(result, 5));
  EXPECT_EQ("4000", TestingSQLUtil::GetResultValueAsString(result, 7));

  // free the database just created
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
