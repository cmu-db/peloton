//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varchar_sql_test.cpp
//
// Identification: test/sql/varchar_sql_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class VarcharSQLTests : public PelotonTest {};

void PopulateVarcharTable() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(name varchar(250));");
  std::vector<std::string> names{"Alice", "Peter",  "Cathy",
                                 "Bob",   "Alicia", "David"};
  for (size_t i = 0; i < names.size(); i++) {
    std::string sql = "INSERT INTO foo VALUES ('" + names[i] + "');";
    TestingSQLUtil::ExecuteSQLQuery(sql);
  }
}

void CleanUp() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

void CheckVarcharQueryResult(std::vector<StatementResult> result,
                             std::vector<std::string> expected,
                             size_t tuple_descriptor_size) {
  EXPECT_EQ(result.size(), expected.size());
  for (size_t i = 0; i < result.size(); i++) {
    for (size_t j = 0; j < tuple_descriptor_size; j++) {
      int idx = i * tuple_descriptor_size + j;
      std::string s =
          std::string(result[idx].second.begin(), result[idx].second.end());
      EXPECT_TRUE(std::find(expected.begin(), expected.end(), s) !=
                  expected.end());
    }
  }
}

TEST_F(VarcharSQLTests, VarcharEqualityTest) {
  PopulateVarcharTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  std::string query;
  std::vector<std::string> expected{};

  query = "SELECT * FROM foo WHERE name = 'Alice';";
  expected = {"Alice"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'david';";
  expected = {};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'Ann';";
  expected = {};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'Alice' OR name = 'Alicia';";
  expected = {"Alice", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name != 'Bob' AND name != 'David';";
  expected = {"Alicia", "Alice", "Peter", "Cathy"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  CleanUp();
}

TEST_F(VarcharSQLTests, VarcharRangeTest) {
  PopulateVarcharTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  std::string query;
  std::vector<std::string> expected{};

  query = "SELECT * FROM foo WHERE name >= 'A' AND name <= 'D';";
  expected = {"Alice", "Alicia", "Bob", "Cathy"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name > 'David';";
  expected = {"Peter"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name <= 'Alicia';";
  expected = {"Alice", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckVarcharQueryResult(result, expected, tuple_descriptor.size());

  CleanUp();
}

}  // namespace test
}  // namespace peloton
