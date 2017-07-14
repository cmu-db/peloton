//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_sql_test.cpp
//
// Identification: test/sql/insert_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "common/harness.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

class InsertSQLTests : public PelotonTest {};

void CreateTable() {
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT, b CHAR(4), c VARCHAR(10), d VARCHAR(2));");

  }

TEST_F(InsertSQLTests, InsertCorrectTuplesTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  CreateTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES(1, 'abcd', 'abcdefghij', 'ab');", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);
  EXPECT_EQ(1, rows_changed);

  TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                  tuple_descriptor, rows_changed,
                                  error_message);

  // Check the return value
  // Should be: 22, 33
  EXPECT_EQ(0, rows_changed);
  EXPECT_EQ("1", TestingSQLUtil::GetResultValueAsString(result, 0));
  EXPECT_EQ("abcd", TestingSQLUtil::GetResultValueAsString(result, 1));
  EXPECT_EQ("abcdefghij", TestingSQLUtil::GetResultValueAsString(result, 2));
  EXPECT_EQ("ab", TestingSQLUtil::GetResultValueAsString(result, 3));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertSQLTests, InsertTuplesBiggerThanColumnTest) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    CreateTable();

    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES(1, 'abcd', 'abcdefghij', 'abcd');", result,
                                    tuple_descriptor, rows_changed,
                                    error_message);
    EXPECT_EQ(0, rows_changed);

    TestingSQLUtil::ExecuteSQLQuery("SELECT * FROM test;", result,
                                    tuple_descriptor, rows_changed,
                                    error_message);

    // Check the return value
    // Should be: 22, 33
    EXPECT_EQ(0, rows_changed);
    EXPECT_EQ(0, result.size());

    // free the database just created
    txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
