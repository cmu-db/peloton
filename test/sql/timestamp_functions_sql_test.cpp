//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_sql_test.cpp
//
// Identification: test/sql/timestamp_functions_sql_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class TimestampFunctionsSQLTest : public PelotonTest {};

TEST_F(TimestampFunctionsSQLTest, DateTruncTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, value timestamp);");
  // Add in a testing timestamp
  TestingSQLUtil::ExecuteSQLQuery(
      "insert into foo values(3, '2016-12-07 13:26:02.123456-05');");

  txn_manager.CommitTransaction(txn);

  std::string test_query;
  std::vector<std::string> expected;
  std::vector<ResultValue> result;

  // wrong argument type
  test_query = "select date_trunc(123, value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  EXPECT_EQ(0, result.size());

  // wrong DatePartType
  test_query = "select date_trunc('abc', value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  EXPECT_EQ(0, result.size());

  // Test a few end-to-end DatePartType strings. The correctness of the function
  // is already tested in the unit tests.
  test_query = "select date_trunc('minute', value) from foo;";
  expected = {"2016-12-07 13:26:00.000000-05"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select date_trunc('DAY', value) from foo;";
  expected = {"2016-12-07 00:00:00.000000-05"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select date_trunc('CenTuRy', value) from foo;";
  expected = {"2001-01-01 00:00:00.000000-05"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(TimestampFunctionsSQLTest, DatePartTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, value timestamp);");
  // Add in a testing timestamp
  TestingSQLUtil::ExecuteSQLQuery(
      "insert into foo values(3, '2016-12-07 13:26:02.123456-05');");

  txn_manager.CommitTransaction(txn);

  std::string test_query;
  std::vector<std::string> expected;
  std::vector<ResultValue> result;

  // wrong argument type
  test_query = "select date_part('123', value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  EXPECT_EQ(0, result.size());

  // wrong DatePartType
  test_query = "select date_part('abc', value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  EXPECT_EQ(0, result.size());

  test_query = "select extract(abc from value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result);
  EXPECT_EQ(0, result.size());

  // Test a few end-to-end DatePartType strings. The correctness of the function
  // is already tested in the unit tests.
  test_query = "select date_part('minute', value) from foo;";
  expected = {"26"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select extract(minute from value) from foo;";
  expected = {"26"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select date_part('DAY', value) from foo;";
  expected = {"7"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select extract(DAY from value) from foo;";
  expected = {"7"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select date_part('CenTuRy', value) from foo;";
  expected = {"21"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  test_query = "select extract(CenTuRy from value) from foo;";
  expected = {"21"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(test_query.c_str(), expected,
                                                false);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
