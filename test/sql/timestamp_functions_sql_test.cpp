//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_sql_test.cpp
//
// Identification: test/sql/timestamp_functions_sql_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
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

  // Fetch values from the table
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // wrong argument type
  std::string test_query = "select date_trunc(123, value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  EXPECT_EQ(result.size(), 0);

  // wrong DatePartType
  test_query = "select date_trunc('abc', value) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  EXPECT_EQ(result.size(), 0);

  // Test a few end-to-end DatePartType strings. The correctness of the function
  // is already tested in the unit tests.
  test_query = "select date_trunc('minute', value) from foo;";
  std::string expected = "2016-12-07 13:26:00.000000-05";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  auto query_result = TestingSQLUtil::GetResultValueAsString(result, 0);
  EXPECT_EQ(query_result, expected);

  test_query = "select date_trunc('DAY', value) from foo;";
  expected = "2016-12-07 00:00:00.000000-05";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  query_result = TestingSQLUtil::GetResultValueAsString(result, 0);
  EXPECT_EQ(query_result, expected);

  test_query = "select date_trunc('CenTuRy', value) from foo;";
  expected = "2001-01-01 00:00:00.000000-05";
  TestingSQLUtil::ExecuteSQLQuery(test_query.c_str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  query_result = TestingSQLUtil::GetResultValueAsString(result, 0);
  EXPECT_EQ(query_result, expected);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
